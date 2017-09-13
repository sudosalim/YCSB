/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package com.yahoo.ycsb.db;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSON;
import com.yahoo.ycsb.*;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.generator.soe.PredicateGenerator;
import com.yahoo.ycsb.generator.soe.PredicateSequence;
import org.bson.Document;
import org.bson.types.Binary;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends DB {


  private int soeLimitMin;
  private int soeLimitMax;
  private int soeOffsetMin;
  private int soeOffsetMax;
  private Random rnd = new Random();

  /** Used to include a field in a response. */
  private static final Integer INCLUDE = Integer.valueOf(1);

  /** The options to use for inserting many documents. */
  private static final InsertManyOptions INSERT_UNORDERED =
      new InsertManyOptions().ordered(false);

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions()
      .upsert(true);

  /**
   * The database name to access.
   */
  private static String databaseName;

  /** The database name to access. */
  private static MongoDatabase database;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** A singleton Mongo instance. */
  private static MongoClient mongoClient;

  /** The default read preference for the test. */
  private static ReadPreference readPreference;

  /** The default write concern for the test. */
  private static WriteConcern writeConcern;

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The bulk inserts pending for the thread. */
  private final List<Document> bulkInserts = new ArrayList<Document>();

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (Exception e1) {
        System.err.println("Could not close MongoDB connection pool: "
            + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        database = null;
        mongoClient = null;
      }
    }
  }

  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      DeleteResult result =
          collection.withWriteConcern(writeConcern).deleteOne(query);
      if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
        System.err.println("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongoClient != null) {
        return;
      }

      Properties props = getProperties();

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(
          props.getProperty("mongodb.upsert", "false"));


      soeLimitMin = Integer.parseInt(props.getProperty("soe_querylimit_min", "0"));
      soeLimitMax = Integer.parseInt(props.getProperty("soe_querylimit_max", "100"));
      soeOffsetMin = Integer.parseInt(props.getProperty("soe_offset_min", "0"));
      soeOffsetMax = Integer.parseInt(props.getProperty("soe_offset_max", "100"));

      if (soeLimitMin > soeLimitMax) {
        int tmp = soeLimitMin;
        soeLimitMin = soeLimitMax;
        soeLimitMax = tmp;
      }

      if (soeOffsetMin > soeOffsetMax) {
        int tmp = soeOffsetMin;
        soeOffsetMin = soeOffsetMax;
        soeOffsetMax = tmp;
      }


      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://")) {
        System.err.println("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options'. "
            + "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      try {
        MongoClientURI uri = new MongoClientURI(url);

        String uriDb = uri.getDatabase();
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
            && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        } else {
          // If no database is specified in URI, use "ycsb"
          databaseName = "ycsb";

        }

        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();

        mongoClient = new MongoClient(uri);
        database =
            mongoClient.getDatabase(databaseName)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);

        System.out.println("mongo client connection created with " + url);
      } catch (Exception e1) {
        System.err
            .println("Could not initialize MongoDB connection pool for Loader: "
                + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document toInsert = new Document("_id", key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.put(entry.getKey(), entry.getValue().toArray());
      }

      if (batchSize == 1) {
        if (useUpsert) {
          // this is effectively an insert, but using an upsert instead due
          // to current inability of the framework to clean up after itself
          // between test runs.
          collection.replaceOne(new Document("_id", toInsert.get("_id")),
              toInsert, UPDATE_WITH_UPSERT);
        } else {
          collection.insertOne(toInsert);
        }
      } else {
        bulkInserts.add(toInsert);
        if (bulkInserts.size() == batchSize) {
          if (useUpsert) {
            List<UpdateOneModel<Document>> updates = 
                new ArrayList<UpdateOneModel<Document>>(bulkInserts.size());
            for (Document doc : bulkInserts) {
              updates.add(new UpdateOneModel<Document>(
                  new Document("_id", doc.get("_id")),
                  doc, UPDATE_WITH_UPSERT));
            }
            collection.bulkWrite(updates);
          } else {
            collection.insertMany(bulkInserts, INSERT_UNORDERED);
          }
          bulkInserts.clear();
        } else {
          return Status.BATCHED_OK;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);

      FindIterable<Document> findIterable = collection.find(query);

      if (fields != null) {
        Document projection = new Document();
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
      }

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
      }
      Document update = new Document("$set", fieldsToSet);

      UpdateResult result = collection.updateOne(query, update);
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }


  // *********************  SOE Insert ********************************

  @Override
  public Status soeInsert(String table, HashMap<String, ByteIterator> result, PredicateGenerator gen)  {

    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String docId = gen.getSequentialDocId();
      String docRawBody = gen.getRandomDocument();
      Document toInsert = new Document("_id", docId);
      DBObject body = (DBObject) JSON.parse(docRawBody);
      toInsert.put(docId, body);
      collection.insertOne(toInsert);
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  // *********************  SOE Update ********************************

  @Override
  public Status soeUpdate(String table, HashMap<String, ByteIterator> result, PredicateGenerator gen) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      String docId = gen.getRandomDocId();

      PredicateSequence predicateSequence = gen.getSearchPredicateSequnce();
      String addrCountry = predicateSequence.getName();
      String addrCountryValue = predicateSequence.getValueA();

      Document query = new Document("_id", docId);
      Document fieldsToSet = new Document();

      fieldsToSet.put(addrCountry, addrCountryValue);
      Document update = new Document("$set", fieldsToSet);

      UpdateResult res = collection.updateOne(query, update);
      if (res.wasAcknowledged() && res.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + docId);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  // *********************  SOE Scan ********************************
  @Override
  public Status soeScan(String table, final Vector<HashMap<String, ByteIterator>> result, PredicateGenerator gen) {
    int limit = getSoeLimit();
    int offset = getSoeOffset();
    String key = gen.getRandomDocId();
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", key);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(limit).skip(offset);

      Document projection = new Document();
      for (String field : gen.getAllFieldsProjection()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + key);
        return Status.ERROR;
      }

      result.ensureCapacity(limit);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  // *********************  SOE Page ********************************

  @Override
  public Status soePage(String table, final Vector<HashMap<String, ByteIterator>> result, PredicateGenerator gen) {

    PredicateSequence predicateSequence = gen.getPagePredicateSequence();
    int limit = getSoeLimit();
    int offset = getSoeOffset();
    String addrContry = predicateSequence.getName();
    String addrCounryValue = predicateSequence.getValueA();

    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document(addrContry, addrCounryValue);

      FindIterable<Document> findIterable =
          collection.find(query).limit(limit).skip(offset);

      Document projection = new Document();
      for (String field : gen.getAllFieldsProjection()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }

      result.ensureCapacity(limit);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();
        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

// *********************  SOE search ********************************

  @Override
  public Status soeSearch(String table, final Vector<HashMap<String, ByteIterator>> result, PredicateGenerator gen) {

    MongoCursor<Document> cursor = null;
    try {
      int recordcount = getSoeLimit();
      int offset = getSoeOffset();

      PredicateSequence predicateSequence = gen.getSearchPredicateSequnce();
      String addrCountry = predicateSequence.getName();
      String addrCountryValue = predicateSequence.getValueA();
      String agegroup = predicateSequence.getNestedPredicate().getName();
      String agegroupValue = predicateSequence.getNestedPredicate().getValueA();
      String dobyear = predicateSequence.getNestedPredicate().getNestedPredicate().getName();
      String dobyearValue = predicateSequence.getNestedPredicate().getNestedPredicate().getValueA();


      MongoCollection<Document> collection = database.getCollection(table);

      DBObject clause1 = new BasicDBObject(addrCountry, addrCountryValue);
      DBObject clause2 = new BasicDBObject(agegroup, agegroupValue);
      DBObject clause3Range =
          new BasicDBObject("$gte", new SimpleDateFormat("yyyy-MM-dd").parse(dobyearValue + "-1-1"));
      DBObject clause3 = new BasicDBObject(dobyear, clause3Range);
      DBObject clause4Range =
          new BasicDBObject("$lte", new SimpleDateFormat("yyyy-MM-dd").parse(dobyearValue+ "-12-31"));
      DBObject clause4 = new BasicDBObject(dobyear, clause4Range);

      BasicDBList and = new BasicDBList();
      and.add(clause1);
      and.add(clause2);
      and.add(clause3);
      and.add(clause4);

      Document query = new Document("$and", and);

      FindIterable<Document> findIterable =
          collection.find(query).sort(new BasicDBObject(addrCountry, 1).
              append(agegroup, 1).append(dobyear, 1)).limit(recordcount).skip(offset);

      Document projection = new Document();
      for (String field : gen.getAllFieldsProjection()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;

    } catch (Exception e) {
      System.out.println(e.getMessage().toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }


  // *********************  SOE NestScan ********************************

  @Override
  public Status soeNestScan(String table, final Vector<HashMap<String, ByteIterator>> result, PredicateGenerator gen) {

    int recordcount = getSoeLimit();
    int offset = getSoeOffset();
    PredicateSequence predicateSequence = gen.getNestScanPredicateSequence();


    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document(predicateSequence.getName(), predicateSequence.getValueA());

      FindIterable<Document> findIterable =
          collection.find(query).limit(recordcount).skip(offset);

      Document projection = new Document();
      for (String field : gen.getAllFieldsProjection()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  // *********************  SOE ArrayScan ********************************

  @Override
  public Status soeArrayScan(String table, final Vector<HashMap<String, ByteIterator>> result, PredicateGenerator gen) {
    int recordcount = getSoeLimit();
    int offset = getSoeOffset();
    PredicateSequence predicateSequence = gen.getArrayScanPredicateSequence();

    Document sort = new Document("_id", INCLUDE);

    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      BasicDBObject   query = new BasicDBObject();
      query.put(predicateSequence.getName(), predicateSequence.getValueA());

      FindIterable<Document> findIterable = collection.find(query).sort(sort).limit(recordcount).skip(offset);
      Document projection = new Document();
      for (String field : gen.getAllFieldsProjection()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);
      cursor = findIterable.iterator();
      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

// *********************  SOE ArrayDeepScan ********************************

  @Override
  public Status soeArrayDeepScan(String table, final Vector<HashMap<String, ByteIterator>> result,
                                 PredicateGenerator gen) {
    int recordcount = getSoeLimit();
    int offset = getSoeOffset();
    PredicateSequence predicateSequence = gen.getArrayDeepScanPredicateSequence();
    String visitedPlacesCountry = predicateSequence.getName();
    String visitedPlacesCountryValue = predicateSequence.getValueA();
    String visitedPlacesActivity = predicateSequence.getNestedPredicate().getName();
    String visitedPlacesActivityValue = predicateSequence.getNestedPredicate().getValueA();

    Document sort = new Document("_id", INCLUDE);

    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      DBObject clause1 = new BasicDBObject(visitedPlacesCountry, visitedPlacesCountryValue);
      DBObject clause2 = new BasicDBObject(visitedPlacesActivity, visitedPlacesActivityValue);

      BasicDBList and = new BasicDBList();
      and.add(clause1);
      and.add(clause2);
      Document query = new Document("$and", and);

      FindIterable<Document> findIterable = collection.find(query).sort(sort).limit(recordcount).skip(offset);

      Document projection = new Document();
      for (String field : gen.getAllFieldsProjection()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);
      cursor = findIterable.iterator();
      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }


  // *********************  SOE Report ********************************

  @Override
  public Status soeReport(String table, final Vector<HashMap<String, ByteIterator>> result, PredicateGenerator gen) {
    int recordcount = getSoeLimit();
    int offset = getSoeOffset();
    PredicateSequence predicateSequence = gen.getReport1PrediateSequence();
    String addrCountry = predicateSequence.getName();
    String addrCountryValue = predicateSequence.getValueA();
    String orderlist = predicateSequence.getNestedPredicate().getName();

    MongoCursor<Document> cursor = null;
    try {

      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document(addrCountry, addrCountryValue);
      FindIterable<Document> findIterable = collection.find(query).limit(offset);
      Document projection = new Document();
      for (String field : gen.getAllFieldsProjection()) {
        projection.put(field, INCLUDE);
      }
      findIterable.projection(projection);
      cursor = findIterable.iterator();
      if (!cursor.hasNext()) {
        return Status.NOT_FOUND;
      }
      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
        Document obj = cursor.next();
        if (obj.get(orderlist) != null) {
          BasicDBObject subq  = new BasicDBObject();
          subq.put("_id", new BasicDBObject("$in", obj.get(orderlist)));
          FindIterable<Document> findSubIterable = collection.find(subq);
          Document orderDoc = findSubIterable.first();
          obj.put(orderlist, orderDoc);
        }
        soeFillMap(resultMap, obj);
        result.add(resultMap);
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }


  private int getSoeLimit() {
    int diff = soeLimitMax - soeLimitMin;
    if (diff != 0) {
      return rnd.nextInt(diff) + soeLimitMin;
    }
    return soeLimitMin;
  }

  private int getSoeOffset() {
    int diff = soeOffsetMax - soeOffsetMin;
    if (diff !=0) {
      return rnd.nextInt(diff) + soeOffsetMin;
    }
    return soeOffsetMin;
  }

  protected void soeFillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      String value = "null";
      if (entry.getValue() != null) {
        value = entry.getValue().toString();
      }
      resultMap.put(entry.getKey(), new StringByteIterator(value));
    }
  }


  /**
   * Fills the map with the values from the DBObject.
   * 
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }

  }
}
