/**
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

import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.service.KeyValueServiceConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.transactions.*;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;

import com.yahoo.ycsb.DBException;

import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.util.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.couchbase.client.core.error.KeyNotFoundException;


/**
 * Full YCSB implementation based on the new Couchbase Java SDK 3.x.
 */
public class Couchbase3Client extends DB {

  private static final String KEY_SEPARATOR = ":";

  private volatile ClusterEnvironment environment;
  private volatile Cluster cluster;
  private volatile Collection collection;
  private static Transactions transactions;
  private static boolean transactionEnabled;
  private int[] transactionKeys;

  private DurabilityLevel durabilityLevel;

  private TransactionDurabilityLevel transDurabilityLevel;
  //private  volatile  HashSet errors = new HashSet<Throwable>();

  @Override
  public synchronized void init() {
    if (environment == null) {
      Properties props = getProperties();

      String hostname = props.getProperty("couchbase.host", "127.0.0.1");
      String bucketName = props.getProperty("couchbase.bucket", "ycsb");
      String username = props.getProperty("couchbase.username", "Administrator");
      String password = props.getProperty("couchbase.password", "password");
      int kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));
      int numATRS = Integer.parseInt(props.getProperty("couchbase.atrs", "1024"));
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
          .builder(hostname, username, password)
          .serviceConfig(ServiceConfig.keyValueServiceConfig(KeyValueServiceConfig.builder().endpoints(kvEndpoints)))
          .build();
      cluster = Cluster.connect(environment);
      Bucket bucket = cluster.bucket(bucketName);
      collection = bucket.defaultCollection();
      if ((transactions == null) && transactionEnabled) {
        transactions = Transactions.create(cluster, TransactionConfigBuilder.create()
            .durabilityLevel(transDurabilityLevel)
            .numATRs(numATRS)
            .build());
      }
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
      return DurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER;
    case 3:
      return DurabilityLevel.PERSIST_TO_MAJORITY;
    default :
      throw new DBException("\"couchbase.durability\" must be between 0 and 3");
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
      return TransactionDurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER;
    case 3:
      return TransactionDurabilityLevel.PERSIST_TO_MAJORITY;
    default :
      throw new DBException("\"couchbase.durability\" must be between 0 and 3");
    }
  }

  @Override
  public synchronized void cleanup() {
    if (environment != null) {
      cluster.shutdown();
      environment.shutdown();
      environment = null;
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      GetResult document = collection.get(formatId(table, key));
      //if (!document.isPresent()) {
      //  return Status.NOT_FOUND;
      //}
      extractFields(document.contentAsObject(), fields, result);
      return Status.OK;
    } catch (KeyNotFoundException e) {
      System.out.println("Key NOT_FOUND");
      return Status.NOT_FOUND;
    } catch (Throwable e){
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

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      collection.replace(formatId(table, key), encode(values),
          replaceOptions().durability(durabilityLevel));
      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {

      collection.insert(formatId(table, key), encode(values),
          insertOptions().durability(durabilityLevel));
      return Status.OK;
    } catch (Throwable t) {
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

    try {
      for (int i=0; i<transationKeys.length; i++) {
        switch (transationOperations[i]) {
        case "TRREAD":
          try {
            GetResult document = collection.get(formatId(table, transationKeys[i]));
            //if (!document.isPresent()) {
            //  return Status.NOT_FOUND;
            //}
            System.out.println("calling transaction read");
            extractFields(document.contentAsObject(), fields, result);
          } catch (KeyNotFoundException e) {
            System.out.println("Key NOT_FOUND");
            return Status.NOT_FOUND;
          } catch (Throwable e){
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


  public Status transactionContext(String table, String[] transationKeys, Map<String, ByteIterator>[] transationValues,
                            String[] transationOperations, Set<String> fields, Map<String, ByteIterator> result) {

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
      System.err.println("Transaction failed " + e.result().transactionId() + " " +
          e.result().timeTaken().toMillis() + "msecs");
      for (LogDefer err : e.result().log().logs()) {
        String s = err.toString();
        logger.warn("Transaction failed:" + s);
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
      collection.remove(formatId(table, key),
          removeOptions().durability(durabilityLevel));
      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
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


}
