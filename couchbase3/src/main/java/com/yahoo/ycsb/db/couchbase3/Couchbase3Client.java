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
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.util.*;
import java.util.stream;

/**
 * Full YCSB implementation based on the new Couchbase Java SDK 3.x.
 */
public class Couchbase3Client extends DB {

  private static final String KEY_SEPARATOR = ":";

  private volatile ClusterEnvironment environment;
  private volatile Cluster cluster;
  private volatile Collection collection;


  private int[] transactionKeys;

  @Override
  public synchronized void init() {
    if (environment == null) {
      Properties props = getProperties();

      String hostname = props.getProperty("couchbase.host", "127.0.0.1");
      String bucketName = props.getProperty("couchbase.bucket", "ycsb");
      String username = props.getProperty("couchbase.username", "Administrator");
      String password = props.getProperty("couchbase.password", "password");
      int kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));

      environment = ClusterEnvironment
          .builder(hostname, username, password)
          .serviceConfig(ServiceConfig.keyValueServiceConfig(KeyValueServiceConfig.create(kvEndpoints)))
          .build();
      cluster = Cluster.connect(environment);
      Bucket bucket = cluster.bucket(bucketName);
      collection = bucket.defaultCollection();
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
    Optional<GetResult> document = collection.get(formatId(table, key));
    if (!document.isPresent()) {
      return Status.NOT_FOUND;
    }
    extractFields(document.get().contentAsObject(), fields, result);
    return Status.OK;
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
      collection.replace(formatId(table, key), encode(values));
      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      collection.insert(formatId(table, key), encode(values));
      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
  }

  @Override
  public Status transaction(String table, String[] operations,  String[] keys, Set<String> fields,
                            Map<String, ByteIterator> values) {

    int operationsCount = operations.length;
    int documentsTotal = keys.length;

    try {


      collection.replace(formatId(table, key), encode(values));
      return Status.OK;
    } catch (Throwable t) {
      return Status.ERROR;
    }
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
      collection.remove(formatId(table, key));
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


  private int nextTransactionKey(int totalKeys){
    if (transactionKeys == null) {
      transactionKeys = IntStream.rangeClosed(0, totalKeys).toArray();

    }



  }

}
