/*
 * Copyright (c) 2019 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.couchbase3;

import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.transactions.*;

import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;

import com.couchbase.client.java.ClusterOptions;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.soe.Generator;

import java.time.Duration;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Full YCSB implementation based on the new Couchbase Java SDK 3.x.
 */
public class Couchbase3Client extends DB {

  private static final String KEY_SEPARATOR = ":";

  private static volatile ClusterEnvironment environment;
  private static final AtomicInteger OPEN_CLIENTS = new AtomicInteger(0);
  private static final Object INIT_COORDINATOR = new Object();

  private volatile Cluster cluster;
  private static volatile Bucket bucket;
  private static volatile ClusterOptions clusterOptions;
  //private volatile Collection collectiont;
  private Transactions transactions;
  private boolean transactionEnabled;
  private int[] transactionKeys;

  private volatile TransactionDurabilityLevel transDurabilityLevel;

  private volatile DurabilityLevel durabilityLevel;
  private volatile PersistTo persistTo;
  private volatile ReplicateTo replicateTo;
  private volatile boolean useDurabilityLevels;
  private  volatile  HashSet errors = new HashSet<Throwable>();

  private boolean adhoc;
  private int maxParallelism;
  private String scanAllQuery;
  private String bucketName;

  private static boolean collectionenabled;
  private static String username;
  private static String password;
  private static String hostname;
  private static int kvPort;
  private static int managerPort;
  private static long kvTimeoutMillis;
  private static int kvEndpoints;

  private String soeQuerySelectIDClause;
  private String soeQuerySelectAllClause;
  private String soeScanN1qlQuery;
  private String soeScanKVQuery;
  private String soeInsertN1qlQuery;
  private String soeReadN1qlQuery;
  private Boolean isSOETest;

  private boolean upsert;
  private boolean syncMutResponse;
  private boolean epoll;
  private boolean kv;
  private int queryEndpoints;
  private int boost;
  private int networkMetricsInterval;
  private int runtimeMetricsInterval;


  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    isSOETest = props.getProperty("couchbase.soe", "false").equals("true");

    upsert = props.getProperty("couchbase.upsert", "false").equals("true");
    syncMutResponse = props.getProperty("couchbase.syncMutationResponse", "true").equals("true");
    kv = props.getProperty("couchbase.kv", "true").equals("true");
    queryEndpoints = Integer.parseInt(props.getProperty("couchbase.queryEndpoints", "1"));
    epoll = props.getProperty("couchbase.epoll", "false").equals("true");
    boost = Integer.parseInt(props.getProperty("couchbase.boost", "3"));
    networkMetricsInterval = Integer.parseInt(props.getProperty("couchbase.networkMetricsInterval", "0"));
    runtimeMetricsInterval = Integer.parseInt(props.getProperty("couchbase.runtimeMetricsInterval", "0"));

    boolean outOfOrderExecution = Boolean.parseBoolean(props.getProperty("couchbase.outOfOrderExecution", "false"));
    if (outOfOrderExecution) {
      System.setProperty("com.couchbase.unorderedExecutionEnabled", "true");
    } else {
      System.setProperty("com.couchbase.unorderedExecutionEnabled", "false");
    }

    // durability options
    String rawDurabilityLevel = props.getProperty("couchbase.durability", null);
    if (rawDurabilityLevel == null) {
      persistTo = parsePersistTo(props.getProperty("couchbase.persistTo", "0"));
      replicateTo = parseReplicateTo(props.getProperty("couchbase.replicateTo", "0"));
      useDurabilityLevels = false;
    } else {
      durabilityLevel = parseDurabilityLevel(rawDurabilityLevel);
      useDurabilityLevels = true;
    }

    adhoc = props.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(props.getProperty("couchbase.maxParallelism", "1"));

    scanAllQuery =  "SELECT RAW meta().id FROM `" + bucketName +
        "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";
    soeQuerySelectIDClause = "SELECT RAW meta().id FROM";
    soeQuerySelectAllClause = "SELECT RAW `" + bucketName + "` FROM ";
    soeReadN1qlQuery = soeQuerySelectAllClause + " `" + bucketName + "` USE KEYS [$1]";
    soeInsertN1qlQuery = "INSERT INTO `" + bucketName
          + "`(KEY,VALUE) VALUES ($1,$2)";
    soeScanN1qlQuery =  soeQuerySelectAllClause + " `" + bucketName +
          "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";

    soeScanKVQuery =  soeQuerySelectIDClause + " `" + bucketName +
          "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";


    bucketName = props.getProperty("couchbase.bucket", "default");

    int numATRS = Integer.parseInt(props.getProperty("couchbase.atrs", "20480"));

    hostname = props.getProperty("couchbase.host", "127.0.0.1");
    kvPort = Integer.parseInt(props.getProperty("couchbase.kvPort", "11210"));
    managerPort = Integer.parseInt(props.getProperty("couchbase.managerPort", "8091"));
    username = props.getProperty("couchbase.username", "Administrator");

    password = props.getProperty("couchbase.password", "password");

    collectionenabled = props.getProperty(Client.COLLECTION_ENABLED_PROPERTY,
        Client.COLLECTION_ENABLED_DEFAULT).equals("true");

    synchronized (INIT_COORDINATOR) {
      if (environment == null) {

        boolean enableMutationToken = Boolean.parseBoolean(props.getProperty("couchbase.enableMutationToken", "false"));

        kvTimeoutMillis = Integer.parseInt(props.getProperty("couchbase.kvTimeoutMillis", "60000"));
        kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));

        transactionEnabled = Boolean.parseBoolean(props.getProperty("couchbase.transactionsEnabled", "false"));
        try {
          durabilityLevel = parseDurabilityLevel(props.getProperty("couchbase.durability", "0"));
        } catch (DBException e) {
          System.out.println("Failed to parse Durability Level");
        }

        try {
          transDurabilityLevel = parsetransactionDurabilityLevel(props.getProperty("couchbase.durability", "0"));
        } catch (DBException e) {
          System.out.println("Failed to parse TransactionDurability Level");
        }

        environment = ClusterEnvironment
            .builder()
            .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(kvTimeoutMillis)))
            .ioConfig(IoConfig.enableMutationTokens(enableMutationToken).numKvConnections(kvEndpoints))
            .build();

        clusterOptions = ClusterOptions.clusterOptions(username, password);
        clusterOptions.environment(environment);

        Set<SeedNode> seedNodes = new HashSet<>(Arrays.asList(
            SeedNode.create(hostname,
                Optional.of(kvPort),
                Optional.of(managerPort))));

        cluster = Cluster.connect(seedNodes, clusterOptions);
        bucket = cluster.bucket(bucketName);

        if ((transactions == null) && transactionEnabled) {
          transactions = Transactions.create(cluster, TransactionConfigBuilder.create()
              .durabilityLevel(transDurabilityLevel)
              .numATRs(numATRS)
              .build());
        }
      }
    }
    OPEN_CLIENTS.incrementAndGet();
  }

  /**
   * Helper method to log the CLI params so that on the command line debugging is easier.
   */
  private void logParams() {
    StringBuilder sb = new StringBuilder();

    sb.append("host=").append(hostname);
    sb.append(", bucket=").append(bucketName);
    sb.append(", upsert=").append(upsert);
    sb.append(", persistTo=").append(persistTo);
    sb.append(", replicateTo=").append(replicateTo);
    sb.append(", syncMutResponse=").append(syncMutResponse);
    sb.append(", adhoc=").append(adhoc);
    sb.append(", kv=").append(kv);
    sb.append(", maxParallelism=").append(maxParallelism);
    sb.append(", queryEndpoints=").append(queryEndpoints);
    sb.append(", kvEndpoints=").append(kvEndpoints);
    sb.append(", queryEndpoints=").append(queryEndpoints);
    sb.append(", epoll=").append(epoll);
    sb.append(", boost=").append(boost);
    sb.append(", networkMetricsInterval=").append(networkMetricsInterval);
    sb.append(", runtimeMetricsInterval=").append(runtimeMetricsInterval);
    System.out.println("===> Using Params: " + sb.toString());
    //LOGGER.info("===> Using Params: " + sb.toString());
  }

  private static ReplicateTo parseReplicateTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return ReplicateTo.NONE;
    case 1:
      return ReplicateTo.ONE;
    case 2:
      return ReplicateTo.TWO;
    case 3:
      return ReplicateTo.THREE;
    default:
      throw new DBException("\"couchbase.replicateTo\" must be between 0 and 3");
    }
  }

  private static PersistTo parsePersistTo(final String property) throws DBException {
    int value = Integer.parseInt(property);
    switch (value) {
    case 0:
      return PersistTo.NONE;
    case 1:
      return PersistTo.ONE;
    case 2:
      return PersistTo.TWO;
    case 3:
      return PersistTo.THREE;
    case 4:
      return PersistTo.FOUR;
    default:
      throw new DBException("\"couchbase.persistTo\" must be between 0 and 4");
    }
  }

  private static TransactionDurabilityLevel parsetransactionDurabilityLevel(final String property) throws DBException {

    int value = Integer.parseInt(property);

    switch(value){
    case 0:
      return TransactionDurabilityLevel.NONE;
    case 1:
      return TransactionDurabilityLevel.MAJORITY;
    case 2:
      return TransactionDurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
    case 3:
      return TransactionDurabilityLevel.PERSIST_TO_MAJORITY;
    default :
      throw new DBException("\"couchbase.durability\" must be between 0 and 3");
    }
  }

  private static DurabilityLevel parseDurabilityLevel(final String property) throws DBException {

    int value = Integer.parseInt(property);

    switch(value){
    case 0:
      return DurabilityLevel.NONE;
    case 1:
      return DurabilityLevel.MAJORITY;
    case 2:
      return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
    case 3:
      return DurabilityLevel.PERSIST_TO_MAJORITY;
    default :
      throw new DBException("\"couchbase.durability\" must be between 0 and 3");
    }
  }

  @Override
  public synchronized void cleanup() {
    OPEN_CLIENTS.get();
    if (OPEN_CLIENTS.get() == 0 && environment != null) {
      cluster.disconnect();
      environment.shutdown();
      environment = null;
      Iterator<Throwable> it = errors.iterator();
      while(it.hasNext()){
        it.next().printStackTrace();
      }
    }
  }

  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {

    try {

      Collection collection = bucket.defaultCollection();

      GetResult document = collection.get(formatId(table, key));
      extractFields(document.contentAsObject(), fields, result);
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("read failed with exception : " + t);
      return Status.ERROR;
    }
  }

  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result, String scope, String coll) {

    try {

      Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

      GetResult document = collection.get(formatId(table, key));
      extractFields(document.contentAsObject(), fields, result);
      return Status.OK;
    } catch (DocumentNotFoundException e) {
      return Status.NOT_FOUND;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("read failed with exception : " + t);
      return Status.ERROR;
    }
  }

  private static void extractFields(final JsonObject content, Set<String> fields,
                                    final Map<String, ByteIterator> result) {
    if (fields == null || fields.isEmpty()) {
      fields = content.getNames();
    }

    for (String field : fields) {
      result.put(field, new StringByteIterator(content.getString(field)));
    }
  }

  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {

    try {

      Collection collection = bucket.defaultCollection();

      if (useDurabilityLevels) {
        collection.replace(formatId(table, key), encode(values), replaceOptions().durability(durabilityLevel));
      } else {
        collection.replace(formatId(table, key), encode(values), replaceOptions().durability(persistTo, replicateTo));
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  public Status update(final String table, final String key,
                       final Map<String, ByteIterator> values, String scope, String coll) {

    try {

      Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

      if (useDurabilityLevels) {
        collection.replace(formatId(table, key), encode(values), replaceOptions().durability(durabilityLevel));
      } else {
        collection.replace(formatId(table, key), encode(values), replaceOptions().durability(persistTo, replicateTo));
      }
      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      System.err.println("update failed with exception :" + t);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {

    try {
      Collection collection = bucket.defaultCollection();

      if (useDurabilityLevels) {
        collection.insert(formatId(table, key), encode(values), insertOptions().durability(durabilityLevel));
      } else {
        collection.insert(formatId(table, key), encode(values), insertOptions().durability(persistTo, replicateTo));
      }

      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      return Status.ERROR;
    }
  }

  public Status insert(final String table, final String key, final Map<String,
      ByteIterator> values, String scope, String coll) {

    try {

      //System.err.println("scope : " + scope + " : collection :" + coll + " : insert doc : " + key);

      Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

      if (useDurabilityLevels) {
        collection.insert(formatId(table, key), encode(values), insertOptions().durability(durabilityLevel));
      } else {
        collection.insert(formatId(table, key), encode(values), insertOptions().durability(persistTo, replicateTo));
      }

      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      return Status.ERROR;
    }
  }

  @Override
  public Status transaction(String table, String[] transationKeys, Map<String, ByteIterator>[] transationValues,
                            String[] transationOperations, Set<String> fields, Map<String, ByteIterator> result) {
    if (transactionEnabled) {
      return transactionContext(table, transationKeys, transationValues, transationOperations, fields, result);
    }
    return simpleCustomSequense(table, transationKeys, transationValues, transationOperations, fields, result);

  }


  public Status simpleCustomSequense(String table, String[] transationKeys,
                                     Map<String, ByteIterator>[] transationValues, String[] transationOperations,
                                     Set<String> fields, Map<String, ByteIterator> result) {

    Collection collection = bucket.defaultCollection();

    try {

      for (int i = 0; i < transationKeys.length; i++) {
        switch (transationOperations[i]) {
        case "TRREAD":
          try {
            GetResult document = collection.get(formatId(table, transationKeys[i]));
            extractFields(document.contentAsObject(), fields, result);
          } catch (DocumentNotFoundException e) {
            System.out.println("Key NOT_FOUND");
            return Status.NOT_FOUND;
          } catch (Throwable e) {
            return Status.ERROR;
          }
          break;
        case "TRUPDATE":
          collection.replace(formatId(table, transationKeys[i]), encode(transationValues[i]));
          break;
        case "TRINSERT":
          collection.upsert(formatId(table, transationKeys[i]), encode(transationValues[i]));
          break;
        default:
          break;
        }
      }
      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  public Status transactionContext(String table, String[] transationKeys, Map<String,
                                   ByteIterator>[] transationValues,
                                   String[] transationOperations, Set<String> fields,
                                   Map<String, ByteIterator> result) {

    Collection collection = bucket.defaultCollection();

    try {

      transactions.run((ctx) -> {
        // Init and Start transaction here
          for (int i = 0; i < transationKeys.length; i++) {
            final String formattedDocId = formatId(table, transationKeys[i]);
            switch (transationOperations[i]) {
            case "TRREAD":
              TransactionGetResult doc = ctx.get(collection, formattedDocId);
              extractFields(doc.contentAs(JsonObject.class), fields, result);
              break;
            case "TRUPDATE":
              TransactionGetResult docToReplace = ctx.get(collection, formattedDocId);
              JsonObject content = docToReplace.contentAs(JsonObject.class);
              for (Map.Entry<String, String> entry: encode(transationValues[i]).entrySet()){
                content.put(entry.getKey(), entry.getValue());
              }
              ctx.replace(docToReplace, content);
              break;
            case "TRINSERT":
              ctx.insert(collection, formattedDocId, encode(transationValues[i]));
              break;
            default:
              break;
            }
          }
          ctx.commit();
        });
    } catch (TransactionFailed e) {
      Logger logger = LoggerFactory.getLogger(getClass().getName() + ".bad");
      //System.err.println("Transaction failed " + e.result().transactionId() + " " +
          //e.result().timeTaken().toMillis() + "msecs");
      for (LogDefer err : e.result().log().logs()) {
        String s = err.toString();
        logger.warn("transaction failed with exception :" + s);
      }
      return Status.ERROR;
    }
    return Status.OK;
  }


  /**
   * Helper method to turn the passed in iterator values into a map we can encode to json.
   *
   * @param values the values to encode.
   * @return the map of encoded values.
   */
  private static Map<String, String> encode(final Map<String, ByteIterator> values) {
    Map<String, String> result = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      result.put(value.getKey(), value.getValue().toString());
    }
    return result;
  }

  @Override
  public Status delete(final String table, final String key) {
    try {

      Collection collection = bucket.defaultCollection();

      if (useDurabilityLevels) {
        collection.remove(formatId(table, key), removeOptions().durability(durabilityLevel));
      } else {
        collection.remove(formatId(table, key), removeOptions().durability(persistTo, replicateTo));
      }

      return Status.OK;
    } catch (Throwable t) {
      errors.add(t);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      if (fields == null || fields.isEmpty()) {
        return scanAllFields(table, startkey, recordcount, result);
      } else {
        return scanSpecificFields(table, startkey, recordcount, fields, result);
        // need to implement
      }
    } catch (Throwable t) {
      errors.add(t);
      return Status.ERROR;
    }
  }

  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result, String scope, String coll) {
    try {
      if (fields == null || fields.isEmpty()) {
        return scanAllFields(table, startkey, recordcount, result, scope, coll);
      } else {
        return scanSpecificFields(table, startkey, recordcount, fields, result);
        // need to implement
      }
    } catch (Throwable t) {
      errors.add(t);
      return Status.ERROR;
    }
  }

  private Status scanAllFields(final String table, final String startkey, final int recordcount,
                               final Vector<HashMap<String, ByteIterator>> result) {

    Collection collection = bucket.defaultCollection();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);

    cluster.reactive().query(scanAllQuery, queryOptions()
        .parameters(JsonArray.from(formatId(table, startkey), recordcount))
        .adhoc(adhoc).maxParallelism(maxParallelism))
        .flux()
        .flatMap(res -> res.rowsAs(String.class))
        .flatMap(id -> collection.reactive().get(id))
        .map(new Function<GetResult, HashMap<String, ByteIterator>>() {
          @Override
          public HashMap<String, ByteIterator> apply(GetResult getResult) {
            HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
            //System.err.println("printingresult before decoding" + getResult.contentAsObject().toString());
            decode(getResult.contentAsObject().toString(), null, tuple);
            return tuple;
          }
        })
        .toIterable()
        .forEach(new Consumer<HashMap<String, ByteIterator>>() {
          @Override
          public void accept(HashMap<String, ByteIterator> stringByteIteratorHashMap) {
            data.add(stringByteIteratorHashMap);
          }
        });

    return Status.OK;
  }


  private Status scanAllFields(final String table, final String startkey, final int recordcount,
                               final Vector<HashMap<String, ByteIterator>> result, String scope, String coll) {

    Collection collection = collectionenabled ? bucket.scope(scope).collection(coll) : bucket.defaultCollection();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String query =  "SELECT RAW meta().id FROM default:`" + bucketName +
          "`.`" + scope + "`.`"+ coll + "` WHERE meta().id >= $1 ORDER BY meta().id LIMIT $2";
    cluster.reactive().query(query, queryOptions()
          .parameters(JsonArray.from(formatId(table, startkey), recordcount))
          .adhoc(adhoc).maxParallelism(maxParallelism))
          .flux()
          .flatMap(res -> res.rowsAs(String.class))
          .flatMap(id -> collection.reactive().get(id))
          .map(new Function<GetResult, HashMap<String, ByteIterator>>() {
            @Override
            public HashMap<String, ByteIterator> apply(GetResult getResult) {
              HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
              //System.err.println("printingresult before decoding" + getResult.contentAsObject().toString());
              decode(getResult.contentAsObject().toString(), null, tuple);
              return tuple;
            }
          })
          .toIterable()
          .forEach(new Consumer<HashMap<String, ByteIterator>>() {
            @Override
            public void accept(HashMap<String, ByteIterator> stringByteIteratorHashMap) {
              data.add(stringByteIteratorHashMap);
            }
          });

    return Status.OK;
  }

  /**
   * Performs the {@link #scan(String, String, int, Set, Vector)} operation N1Ql only for a subset of the fields.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  private Status scanSpecificFields(final String table, final String startkey, final int recordcount,
                                    final Set<String> fields, final Vector<HashMap<String, ByteIterator>> result) {
    String scanSpecQuery = "SELECT " + joinFields(fields) + " FROM `" + bucketName
        + "` WHERE meta().id >= $1 LIMIT $2";

    QueryResult queryResult = cluster.query(scanSpecQuery, queryOptions()
        .parameters(JsonArray.from(formatId(table, startkey), recordcount))
        .adhoc(adhoc).maxParallelism(maxParallelism));

    boolean allFields = fields == null || fields.isEmpty();
    result.ensureCapacity(recordcount);


    for(JsonObject value : queryResult.rowsAs(JsonObject.class)) {

      if (fields == null) {
        value = value.getObject(bucketName);
      }

      Set<String> f = allFields ? value.getNames() : fields;
      HashMap<String, ByteIterator> tuple =new HashMap<String, ByteIterator>(f.size());
      for (String field : f){
        tuple.put(field, new StringByteIterator(value.getString(field)));
      }
      result.add(tuple);
    }

    return Status.OK;
  }


  private void decode(final String source, final Set<String> fields,
                      final Map<String, ByteIterator> dest) {
    try {
      JsonNode json = JacksonTransformers.MAPPER.readTree(source);
      boolean checkFields = fields != null && !fields.isEmpty();
      for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
        Map.Entry<String, JsonNode> jsonField = jsonFields.next();
        String name = jsonField.getKey();
        if (checkFields && !fields.contains(name)) {
          continue;
        }
        JsonNode jsonValue = jsonField.getValue();
        if (jsonValue != null && !jsonValue.isNull()) {
          dest.put(name, new StringByteIterator(jsonValue.asText()));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not decode JSON");
    }
  }


  /**
   * Helper method to join the set of fields into a String suitable for N1QL.
   *
   * @param fields the fields to join.
   * @return the joined fields as a String.
   */
  private static String joinFields(final Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      return "*";
    }
    StringBuilder builder = new StringBuilder();
    for (String f : fields) {
      builder.append("`").append(f).append("`").append(",");
    }
    String toReturn = builder.toString();
    return toReturn.substring(0, toReturn.length() - 1);
  }

  /**
   * Helper method to turn the prefix and key into a proper document ID.
   *
   * @param prefix the prefix (table).
   * @param key the key itself.
   * @return a document ID that can be used with Couchbase.
   */
  private static String formatId(final String prefix, final String key) {
    return prefix + KEY_SEPARATOR + key;
  }


     /*
    SOE operations.
   */


  @Override
  public Status soeLoad(String table, Generator generator) {

    try {
      String docId = generator.getCustomerIdRandom();
      Collection collection = bucket.defaultCollection();
      GetResult doc = collection.get(docId, GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE));
      if (doc != null) {
        generator.putCustomerDocument(docId, doc.contentAs(String.class));

        try {
          JsonNode json = JacksonTransformers.MAPPER.readTree(doc.contentAs(byte[].class));
          for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
            Map.Entry<String, JsonNode> jsonField = jsonFields.next();
            String name = jsonField.getKey();
            if (name.equals(Generator.SOE_FIELD_CUSTOMER_ORDER_LIST)) {
              JsonNode jsonValue = jsonField.getValue();
              ArrayList<String> orders = new ArrayList<>();
              for (final JsonNode objNode : jsonValue) {
                orders.add(objNode.asText());
              }
              if (orders.size() > 0) {
                String pickedOrder;
                if (orders.size() >1) {
                  Collections.shuffle(orders);
                }
                pickedOrder = orders.get(0);
                GetResult orderDoc = collection.get(pickedOrder,
                      GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE));
                generator.putOrderDocument(pickedOrder, orderDoc.contentAs(String.class));
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Could not decode JSON");
        }

      } else {
        System.err.println("Error getting document from DB: " + docId);
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }


  // *********************  SOE Insert ********************************

  @Override
  public Status soeInsert(String table, HashMap<String, ByteIterator> result, Generator gen)  {

    try {
      //Pair<String, String> inserDocPair = gen.getInsertDocument();

      if (kv) {
        return soeInsertKv(gen);
      } else {
        return soeInsertN1ql(gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeInsertKv(Generator gen) {
    int tries = 60; // roughly 60 seconds with the 1 second sleep, not 100% accurate.
    for(int i = 0; i < tries; i++) {
      try {
        waitForMutationResponse(bucket.async().insert(
              RawJsonDocument.create(gen.getPredicate().getDocid(), documentExpiry, gen.getPredicate().getValueA()),
              persistTo,
              replicateTo
        ));
        return Status.OK;
      } catch (TemporaryFailureException ex) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while sleeping on TMPFAIL backoff.", ex);
        }
      }
    }
    throw new RuntimeException("Still receiving TMPFAIL from the server after trying " + tries + " times. " +
          "Check your server.");
  }

  private Status soeInsertN1ql(Generator gen)
        throws Exception {

    Collection collection = bucket.defaultCollection();

    try {
      QueryResult queryResult = cluster.query(
          soeInsertN1qlQuery,
          queryOptions()
              .parameters(JsonArray.from(gen.getPredicate().getDocid(), JsonObject.fromJson(gen.getPredicate().getValueA())))
              .adhoc(adhoc)
              .maxParallelism(maxParallelism)
      );
      return Status.OK;
    } catch (Exception ex) {
      throw new DBException("Error while parsing N1QL Result. Query: " + soeInsertN1qlQuery, ex);
    }
  }


  // *********************  SOE Update ********************************

  @Override
  public Status soeUpdate(String table, HashMap<String, ByteIterator> result, Generator gen)  {
    try {
      if (kv) {
        return soeUpdateKv(gen);
      } else {
        return soeUpdateN1ql(gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeUpdateKv(Generator gen)  {

    waitForMutationResponse(bucket.async().replace(
          RawJsonDocument.create(gen.getCustomerIdWithDistribution(), documentExpiry, gen.getPredicate().getValueA()),
          persistTo,
          replicateTo
    ));

    return Status.OK;
  }

  private Status soeUpdateN1ql(Generator gen)
        throws Exception {
    String updateQuery = "UPDATE `" + bucketName + "` USE KEYS [$1] SET " +
          gen.getPredicate().getNestedPredicateA().getName() + " = $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          updateQuery,
          JsonArray.from(gen.getCustomerIdWithDistribution(), gen.getPredicate().getNestedPredicateA().getValueA()),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      return Status.ERROR;
    }
    return Status.OK;
  }


  // *********************  SOE Read ********************************
  @Override
  public Status soeRead(String table, HashMap<String, ByteIterator> result, Generator gen) {
    try {
      if (kv) {
        return soeReadKv(result, gen);
      } else {
        return soeReadN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeReadKv(HashMap<String, ByteIterator> result, Generator gen)
        throws Exception {
    RawJsonDocument loaded = bucket.get(gen.getCustomerIdWithDistribution(), RawJsonDocument.class);
    if (loaded == null) {
      return Status.NOT_FOUND;
    }
    soeDecode(loaded.content(), null, result);
    return Status.OK;
  }

  private Status soeReadN1ql(HashMap<String, ByteIterator> result, Generator gen)
        throws Exception {
    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soeReadN1qlQuery,
          JsonArray.from(gen.getCustomerIdWithDistribution()),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException("Error while parsing N1QL Result. Query: " + soeReadN1qlQuery
            + ", Errors: " + queryResult.errors());
    }

    N1qlQueryRow row;
    try {
      row = queryResult.rows().next();
    } catch (NoSuchElementException ex) {
      return Status.NOT_FOUND;
    }

    JsonObject content = row.value();
    Set<String> fields = gen.getAllFields();
    if (fields == null) {
      content = content.getObject(bucketName); // n1ql result set scoped under *.bucketName
      fields = content.getNames();
    }

    for (String field : fields) {
      Object value = content.get(field);
      result.put(field, new StringByteIterator(value != null ? value.toString() : ""));
    }
    return Status.OK;
  }


  // *********************  SOE Scan ********************************

  @Override
  public Status soeScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeScanKv(result, gen);
      } else {
        return soeScanN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeScanKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String key = gen.getCustomerIdWithDistribution();
    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    final Collection collection = bucket.defaultCollection();
    try {
      cluster
          .reactive()
          .query(soeScanKVQuery, queryOptions().adhoc(adhoc).maxParallelism(maxParallelism).parameters(JsonArray.from(key, recordcount)))
          .flatMapMany(res -> res.rowsAs(byte[].class))
          .flatMap(row -> {
            String id = new String(row).trim();
            return collection.reactive().get(id.substring(1, id.length()-1), GetOptions.getOptions().transcoder(RawJsonTranscoder.INSTANCE));
          })
          .map(getResult -> {
            HashMap<String, ByteIterator> tuple = new HashMap<>();
            soeDecode(getResult.contentAs(String.class), null, tuple);
            return tuple;
          })
          .toStream()
          .forEach(data::add);

    } catch (Exception e) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeScanKVQuery, e);
    }

    result.addAll(data);
    return Status.OK;
  }


  private Status soeScanN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    int recordcount = gen.getRandomLimit();

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soeScanN1qlQuery,
          JsonArray.from(gen.getCustomerIdWithDistribution(), recordcount),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeScanN1qlQuery
            + ", Errors: " + queryResult.errors());
    }

    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }

// *********************  SOE search ********************************

  @Override
  public Status soeSearch(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeSearchKv(result, gen);
      } else {
        return soeSearchN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeSearchKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String soeSearchKvQuery = soeQuerySelectIDClause + " `" +  bucketName + "` WHERE " +
          gen.getPredicatesSequence().get(0).getName() + "." +
          gen.getPredicatesSequence().get(0).getNestedPredicateA().getName() + "= $1 AND " +
          gen.getPredicatesSequence().get(1).getName() + " = $2 AND DATE_PART_STR(" +
          gen.getPredicatesSequence().get(2).getName() + ", \"year\") = $3 ORDER BY " +
          gen.getPredicatesSequence().get(0).getName() + "." +
          gen.getPredicatesSequence().get(0).getNestedPredicateA().getName() + ", " +
          gen.getPredicatesSequence().get(1).getName() + ", DATE_PART_STR(" +
          gen.getPredicatesSequence().get(2).getName() + ", \"year\") OFFSET $4 LIMIT $5";

    bucket.async()
          .query(N1qlQuery.parameterized(
                soeSearchKvQuery,
                JsonArray.from(gen.getPredicatesSequence().get(0).getNestedPredicateA().getValueA(),
                      gen.getPredicatesSequence().get(1).getValueA(),
                      Integer.parseInt(gen.getPredicatesSequence().get(2).getValueA()), offset, recordcount),
                N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
          ))
          .doOnNext(new Action1<AsyncN1qlQueryResult>() {
            @Override
            public void call(AsyncN1qlQueryResult result) {
              if (!result.parseSuccess()) {
                throw new RuntimeException("Error while parsing N1QL Result. Query: soeSearchKv(), " +
                      "Errors: " + result.errors());
              }
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryResult, Observable<AsyncN1qlQueryRow>>() {
            @Override
            public Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
              return result.rows();
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryRow, Observable<RawJsonDocument>>() {
            @Override
            public Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
              String id = new String(row.byteValue()).trim();
              return bucket.async().get(id.substring(1, id.length()-1), RawJsonDocument.class);
            }
          })
          .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
            @Override
            public HashMap<String, ByteIterator> call(RawJsonDocument document) {
              HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
              soeDecode(document.content(), null, tuple);
              return tuple;
            }
          })
          .toBlocking()
          .forEach(new Action1<HashMap<String, ByteIterator>>() {
            @Override
            public void call(HashMap<String, ByteIterator> tuple) {
              data.add(tuple);
            }
          });
    result.addAll(data);
    return Status.OK;
  }


  private Status soeSearchN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    String soeSearchN1qlQuery = soeQuerySelectAllClause + " `" +  bucketName + "` WHERE " +
          gen.getPredicatesSequence().get(0).getName() + "." +
          gen.getPredicatesSequence().get(0).getNestedPredicateA().getName() + "= $1 AND " +
          gen.getPredicatesSequence().get(1).getName() + " = $2 AND DATE_PART_STR(" +
          gen.getPredicatesSequence().get(2).getName() + ", \"year\") = $3 ORDER BY " +
          gen.getPredicatesSequence().get(0).getName() + "." +
          gen.getPredicatesSequence().get(0).getNestedPredicateA().getName() + ", " +
          gen.getPredicatesSequence().get(1).getName() + ", DATE_PART_STR(" +
          gen.getPredicatesSequence().get(2).getName() + ", \"year\") OFFSET $4 LIMIT $5";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soeSearchN1qlQuery,
          JsonArray.from(gen.getPredicatesSequence().get(0).getNestedPredicateA().getValueA(),
                gen.getPredicatesSequence().get(1).getValueA(),
                Integer.parseInt(gen.getPredicatesSequence().get(2).getValueA()), offset, recordcount),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeSearchN1qlQuery
            + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }

    return Status.OK;
  }


  // *********************  SOE Page ********************************

  @Override
  public Status soePage(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soePageKv(result, gen);
      } else {
        return soePageN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soePageKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String soePageKvQuery = soeQuerySelectIDClause + " `" +  bucketName + "` WHERE " + gen.getPredicate().getName() +
          "." + gen.getPredicate().getNestedPredicateA().getName() + " = $1 OFFSET $2 LIMIT $3";

    bucket.async()
          .query(N1qlQuery.parameterized(
                soePageKvQuery,
                JsonArray.from(gen.getPredicate().getNestedPredicateA().getValueA(), offset, recordcount),
                N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
          ))
          .doOnNext(new Action1<AsyncN1qlQueryResult>() {
            @Override
            public void call(AsyncN1qlQueryResult result) {
              if (!result.parseSuccess()) {
                throw new RuntimeException("Error while parsing N1QL Result. Query: soePageKv(), " +
                      "Errors: " + result.errors());
              }
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryResult, Observable<AsyncN1qlQueryRow>>() {
            @Override
            public Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
              return result.rows();
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryRow, Observable<RawJsonDocument>>() {
            @Override
            public Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
              String id = new String(row.byteValue()).trim();
              return bucket.async().get(id.substring(1, id.length()-1), RawJsonDocument.class);
            }
          })
          .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
            @Override
            public HashMap<String, ByteIterator> call(RawJsonDocument document) {
              HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
              soeDecode(document.content(), null, tuple);
              return tuple;
            }
          })
          .toBlocking()
          .forEach(new Action1<HashMap<String, ByteIterator>>() {
            @Override
            public void call(HashMap<String, ByteIterator> tuple) {
              data.add(tuple);
            }
          });

    result.addAll(data);

    return Status.OK;
  }


  private Status soePageN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    int recordcount = gen.getRandomLimit();
    int offset = gen.getRandomOffset();

    String soePageN1qlQuery = soeQuerySelectAllClause + " `" +  bucketName + "` WHERE " +
          gen.getPredicate().getName() + "." + gen.getPredicate().getNestedPredicateA().getName() +
          " = $1 OFFSET $2 LIMIT $3";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soePageN1qlQuery,
          JsonArray.from(gen.getPredicate().getNestedPredicateA().getValueA(), offset, recordcount),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soePageN1qlQuery
            + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }


  // *********************  SOE NestScan ********************************

  @Override
  public Status soeNestScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeNestScanKv(result, gen);
      } else {
        return soeNestScanN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeNestScanKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String soeNestScanKvQuery = soeQuerySelectIDClause + " `" +  bucketName + "` WHERE " +
          gen.getPredicate().getName() + "." +
          gen.getPredicate().getNestedPredicateA().getName() + "." +
          gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getName() + " = $1  LIMIT $2";

    bucket.async()
          .query(N1qlQuery.parameterized(
                soeNestScanKvQuery,
                JsonArray.from(
                      gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getValueA(),
                      recordcount),
                N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
          ))
          .doOnNext(new Action1<AsyncN1qlQueryResult>() {
            @Override
            public void call(AsyncN1qlQueryResult result) {
              if (!result.parseSuccess()) {
                throw new RuntimeException("Error while parsing N1QL Result. Query: soeNestedScanKv(), " +
                      "Errors: " + result.errors());
              }
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryResult, Observable<AsyncN1qlQueryRow>>() {
            @Override
            public Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
              return result.rows();
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryRow, Observable<RawJsonDocument>>() {
            @Override
            public Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
              String id = new String(row.byteValue()).trim();
              return bucket.async().get(id.substring(1, id.length()-1), RawJsonDocument.class);
            }
          })
          .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
            @Override
            public HashMap<String, ByteIterator> call(RawJsonDocument document) {
              HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
              soeDecode(document.content(), null, tuple);
              return tuple;
            }
          })
          .toBlocking()
          .forEach(new Action1<HashMap<String, ByteIterator>>() {
            @Override
            public void call(HashMap<String, ByteIterator> tuple) {
              data.add(tuple);
            }
          });

    result.addAll(data);
    return Status.OK;
  }


  private Status soeNestScanN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String soeNestScanN1qlQuery = soeQuerySelectAllClause + " `" +  bucketName + "` WHERE " +
          gen.getPredicate().getName() + "." +
          gen.getPredicate().getNestedPredicateA().getName() + "." +
          gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getName() + " = $1  LIMIT $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soeNestScanN1qlQuery,
          JsonArray.from(gen.getPredicate().getNestedPredicateA().getNestedPredicateA().getValueA(),  recordcount),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeNestScanN1qlQuery
            + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }

    return Status.OK;
  }


  // *********************  SOE ArrayScan ********************************

  @Override
  public Status soeArrayScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeArrayScanKv(result, gen);
      } else {
        return soeArrayScanN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeArrayScanKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String soeArrayScanKvQuery = soeQuerySelectIDClause + " `" +  bucketName + "` WHERE ANY v IN " +
          gen.getPredicate().getName() + " SATISFIES v = $1 END ORDER BY meta().id LIMIT $2";

    bucket.async()
          .query(N1qlQuery.parameterized(
                soeArrayScanKvQuery,
                JsonArray.from(gen.getPredicate().getValueA(),  recordcount),
                N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
          ))
          .doOnNext(new Action1<AsyncN1qlQueryResult>() {
            @Override
            public void call(AsyncN1qlQueryResult result) {
              if (!result.parseSuccess()) {
                throw new RuntimeException("Error while parsing N1QL Result. Query: soeArrayScanKv(), " +
                      "Errors: " + result.errors());
              }
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryResult, Observable<AsyncN1qlQueryRow>>() {
            @Override
            public Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
              return result.rows();
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryRow, Observable<RawJsonDocument>>() {
            @Override
            public Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
              String id = new String(row.byteValue()).trim();
              return bucket.async().get(id.substring(1, id.length()-1), RawJsonDocument.class);
            }
          })
          .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
            @Override
            public HashMap<String, ByteIterator> call(RawJsonDocument document) {
              HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
              soeDecode(document.content(), null, tuple);
              return tuple;
            }
          })
          .toBlocking()
          .forEach(new Action1<HashMap<String, ByteIterator>>() {
            @Override
            public void call(HashMap<String, ByteIterator> tuple) {
              data.add(tuple);
            }
          });

    result.addAll(data);
    return Status.OK;
  }


  private Status soeArrayScanN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();
    String soeArrayScanN1qlQuery = soeQuerySelectAllClause + "`" +  bucketName + "` WHERE ANY v IN " +
          gen.getPredicate().getName() + " SATISFIES v = $1 END ORDER BY meta().id LIMIT $2";
    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soeArrayScanN1qlQuery,
          JsonArray.from(gen.getPredicate().getValueA(),  recordcount),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeArrayScanN1qlQuery
            + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);
    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }

    return Status.OK;
  }


  // *********************  SOE ArrayDeepScan ********************************

  @Override
  public Status soeArrayDeepScan(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeArrayDeepScanKv(result, gen);
      } else {
        return soeArrayDeepScanN1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeArrayDeepScanKv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    final List<HashMap<String, ByteIterator>> data = new ArrayList<HashMap<String, ByteIterator>>(recordcount);
    String visitedPlacesFieldName = gen.getPredicate().getName();
    String countryFieldName = gen.getPredicate().getNestedPredicateA().getName();
    String cityFieldName = gen.getPredicate().getNestedPredicateB().getName();

    String cityCountryValue = gen.getPredicate().getNestedPredicateA().getValueA() + "." +
          gen.getPredicate().getNestedPredicateB().getValueA();

    String soeArrayDeepScanKvQuery =  soeQuerySelectIDClause + " `" +  bucketName + "` WHERE ANY v IN "
          + visitedPlacesFieldName + " SATISFIES  ANY c IN v." + cityFieldName + " SATISFIES (v."
          + countryFieldName + " || \".\" || c) = $1  END END  ORDER BY META().id LIMIT $2";

    bucket.async()
          .query(N1qlQuery.parameterized(
                soeArrayDeepScanKvQuery,
                JsonArray.from(cityCountryValue, recordcount),
                N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
          ))
          .doOnNext(new Action1<AsyncN1qlQueryResult>() {
            @Override
            public void call(AsyncN1qlQueryResult result) {
              if (!result.parseSuccess()) {
                throw new RuntimeException("Error while parsing N1QL Result. Query: soeArrayDeepScanKv(), " +
                      "Errors: " + result.errors());
              }
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryResult, Observable<AsyncN1qlQueryRow>>() {
            @Override
            public Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
              return result.rows();
            }
          })
          .flatMap(new Func1<AsyncN1qlQueryRow, Observable<RawJsonDocument>>() {
            @Override
            public Observable<RawJsonDocument> call(AsyncN1qlQueryRow row) {
              String id = new String(row.byteValue()).trim();
              return bucket.async().get(id.substring(1, id.length()-1), RawJsonDocument.class);
            }
          })
          .map(new Func1<RawJsonDocument, HashMap<String, ByteIterator>>() {
            @Override
            public HashMap<String, ByteIterator> call(RawJsonDocument document) {
              HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>();
              soeDecode(document.content(), null, tuple);
              return tuple;
            }
          })
          .toBlocking()
          .forEach(new Action1<HashMap<String, ByteIterator>>() {
            @Override
            public void call(HashMap<String, ByteIterator> tuple) {
              data.add(tuple);
            }
          });

    result.addAll(data);
    return Status.OK;
  }


  private Status soeArrayDeepScanN1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    int recordcount = gen.getRandomLimit();

    String visitedPlacesFieldName = gen.getPredicate().getName();
    String countryFieldName = gen.getPredicate().getNestedPredicateA().getName();
    String cityFieldName = gen.getPredicate().getNestedPredicateB().getName();

    String cityCountryValue = gen.getPredicate().getNestedPredicateA().getValueA() + "." +
          gen.getPredicate().getNestedPredicateB().getValueA();

    String soeArrayDeepScanN1qlQuery =  soeQuerySelectAllClause + " `" +  bucketName + "` WHERE ANY v IN "
          + visitedPlacesFieldName + " SATISFIES  ANY c IN v." + cityFieldName + " SATISFIES (v."
          + countryFieldName + " || \".\" || c) = $1  END END  ORDER BY META().id LIMIT $2";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soeArrayDeepScanN1qlQuery,
          JsonArray.from(cityCountryValue, recordcount),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeArrayDeepScanN1qlQuery
            + ", Errors: " + queryResult.errors());
    }
    result.ensureCapacity(recordcount);

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }

  // *********************  SOE Report  ********************************

  @Override
  public Status soeReport(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeReport1Kv(result, gen);
      } else {
        return soeReport1N1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeReport1Kv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    return soeReport1N1ql(result, gen);
  }


  private Status soeReport1N1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    String soeReport1N1qlQuery = "SELECT * FROM `" +  bucketName + "` c1 INNER JOIN `" +
          bucketName + "` o1 ON KEYS c1." + gen.getPredicatesSequence().get(0).getName() + " WHERE c1." +
          gen.getPredicatesSequence().get(1).getName() + "." +
          gen.getPredicatesSequence().get(1).getNestedPredicateA().getName()+ " = $1 ";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soeReport1N1qlQuery,
          JsonArray.from(gen.getPredicatesSequence().get(1).getNestedPredicateA().getValueA()),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));
    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeReport1N1qlQuery
            + ", Errors: " + queryResult.errors());
    }

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }

    return Status.OK;
  }

  // *********************  SOE Report 2  ********************************
  @Override
  public Status soeReport2(String table, final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    try {
      if (kv) {
        return soeReport2Kv(result, gen);
      } else {
        return soeReport2N1ql(result, gen);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status soeReport2Kv(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {
    return soeReport2N1ql(result, gen);
  }

  private Status soeReport2N1ql(final Vector<HashMap<String, ByteIterator>> result, Generator gen) {

    String nameOrderMonth = gen.getPredicatesSequence().get(0).getName();
    String nameOrderSaleprice = gen.getPredicatesSequence().get(1).getName();
    String nameAddress =  gen.getPredicatesSequence().get(2).getName();
    String nameAddressZip =  gen.getPredicatesSequence().get(2).getNestedPredicateA().getName();
    String nameOrderlist = gen.getPredicatesSequence().get(3).getName();
    String valueOrderMonth = gen.getPredicatesSequence().get(0).getValueA();
    String valueAddressZip =  gen.getPredicatesSequence().get(2).getNestedPredicateA().getValueA();

    String soeReport2N1qlQuery = "SELECT o2." + nameOrderMonth + ", c2." + nameAddress + "." + nameAddressZip +
          ", SUM(o2." + nameOrderSaleprice + ") FROM `" +  bucketName  + "` c2 INNER JOIN `" +  bucketName +
          "` o2 ON KEYS c2." + nameOrderlist + " WHERE c2." + nameAddress + "." + nameAddressZip +
          " = $1 AND o2." + nameOrderMonth + " = $2 GROUP BY o2." + nameOrderMonth + ", c2." + nameAddress +
          "." + nameAddressZip + " ORDER BY SUM(o2." + nameOrderSaleprice + ")";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          soeReport2N1qlQuery,
          JsonArray.from(valueAddressZip, valueOrderMonth),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));
    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new RuntimeException("Error while parsing N1QL Result. Query: " + soeReport2N1qlQuery
            + ", Errors: " + queryResult.errors());
    }

    for (N1qlQueryRow row : queryResult) {
      HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(gen.getAllFields().size());
      soeDecode(row.value().toString(), null, tuple);
      result.add(tuple);
    }
    return Status.OK;
  }


  /**
   * handling rich JSON types by converting Json arrays and Json objects into String.
   * @param source
   * @param fields
   * @param dest
   */
  private void soeDecode(final String source, final Set<String> fields,
                         final HashMap<String, ByteIterator> dest) {
    try {
      JsonNode json = JacksonTransformers.MAPPER.readTree(source);
      boolean checkFields = fields != null && !fields.isEmpty();
      for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
        Map.Entry<String, JsonNode> jsonField = jsonFields.next();
        String name = jsonField.getKey();
        if (checkFields && !fields.contains(name)) {
          continue;
        }
        JsonNode jsonValue = jsonField.getValue();
        if (jsonValue != null && !jsonValue.isNull()) {
          dest.put(name, new StringByteIterator(jsonValue.toString()));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not soe-decode JSON");
    }
  }




}
