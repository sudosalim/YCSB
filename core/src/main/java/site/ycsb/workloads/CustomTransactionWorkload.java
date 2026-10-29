/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
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

package site.ycsb.workloads;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.RandomByteIterator;
import site.ycsb.WorkloadException;
import site.ycsb.generator.DiscreteGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * CoreWorkload plus multi-document transactions, with the same properties as the couchbase3
 * branch's workload of the same name.
 *
 * <p>Each transaction runs {@code documentsintransaction} operations of one type, chosen per
 * transaction by {@code transactionreadproportion}, {@code transactionupdateproportion} and
 * {@code transactioninsertproportion}. {@code transactionproportion} weighs transactions against
 * the CoreWorkload operations.</p>
 */
public class CustomTransactionWorkload extends CoreWorkload {

  public static final String TRANSACTION_PROPORTION_PROPERTY = "transactionproportion";
  public static final String TRANSACTION_PROPORTION_PROPERTY_DEFAULT = "0";

  public static final String TRANSACTION_READ_PROPORTION_PROPERTY = "transactionreadproportion";
  public static final String TRANSACTION_READ_PROPORTION_PROPERTY_DEFAULT = "0.5";

  public static final String TRANSACTION_UPDATE_PROPORTION_PROPERTY = "transactionupdateproportion";
  public static final String TRANSACTION_UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.5";

  public static final String TRANSACTION_INSERT_PROPORTION_PROPERTY = "transactioninsertproportion";
  public static final String TRANSACTION_INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

  public static final String DOCUMENTS_IN_TRANSACTION_PROPERTY = "documentsintransaction";
  public static final String DOCUMENTS_IN_TRANSACTION_PROPERTY_DEFAULT = "4";

  public static final String TRANSACTION_READ = "TRREAD";
  public static final String TRANSACTION_UPDATE = "TRUPDATE";
  public static final String TRANSACTION_INSERT = "TRINSERT";

  private DiscreteGenerator transactionoperationchooser;
  private int documentsintransaction;
  private List<String> fieldnames;

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);

    documentsintransaction = Integer.parseInt(
        p.getProperty(DOCUMENTS_IN_TRANSACTION_PROPERTY, DOCUMENTS_IN_TRANSACTION_PROPERTY_DEFAULT));

    transactionoperationchooser = new DiscreteGenerator();
    addIfPositive(transactionoperationchooser, p, TRANSACTION_READ_PROPORTION_PROPERTY,
        TRANSACTION_READ_PROPORTION_PROPERTY_DEFAULT, TRANSACTION_READ);
    addIfPositive(transactionoperationchooser, p, TRANSACTION_UPDATE_PROPORTION_PROPERTY,
        TRANSACTION_UPDATE_PROPORTION_PROPERTY_DEFAULT, TRANSACTION_UPDATE);
    addIfPositive(transactionoperationchooser, p, TRANSACTION_INSERT_PROPORTION_PROPERTY,
        TRANSACTION_INSERT_PROPORTION_PROPERTY_DEFAULT, TRANSACTION_INSERT);

    operationchooser = createOperationGenerator(p);
    addIfPositive(operationchooser, p, TRANSACTION_PROPORTION_PROPERTY,
        TRANSACTION_PROPORTION_PROPERTY_DEFAULT, "TRANSACTION");

    String prefix = p.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    fieldnames = new ArrayList<>();
    for (int i = 0; i < fieldcount; i++) {
      fieldnames.add(prefix + i);
    }
  }

  private static void addIfPositive(DiscreteGenerator chooser, Properties p, String property,
                                    String defaultValue, String operation) {
    double weight = Double.parseDouble(p.getProperty(property, defaultValue));
    if (weight > 0) {
      chooser.addValue(weight, operation);
    }
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if (operation == null) {
      return false;
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SCAN":
      doTransactionScan(db);
      break;
    case "TRANSACTION":
      doMultiDocumentTransaction(db);
      break;
    default:
      doTransactionReadModifyWrite(db);
    }
    return true;
  }

  /**
   * Runs one transaction of {@code documentsintransaction} operations of the same type.
   */
  @SuppressWarnings("unchecked")
  public void doMultiDocumentTransaction(DB db) {
    String operation = transactionoperationchooser.nextString();
    String[] keys = new String[documentsintransaction];
    Map<String, ByteIterator>[] values = new HashMap[documentsintransaction];
    String[] operations = new String[documentsintransaction];
    Set<String> fields = null;
    long[] insertedKeys = new long[documentsintransaction];
    int inserted = 0;

    if (TRANSACTION_READ.equals(operation) && !readallfields) {
      fields = new HashSet<>();
      fields.add(fieldnames.get(fieldchooser.nextValue().intValue()));
    }

    for (int i = 0; i < documentsintransaction; i++) {
      operations[i] = operation;
      switch (operation) {
      case TRANSACTION_READ:
        keys[i] = buildKeyName(nextKeynum(), zeropadding, orderedinserts);
        break;
      case TRANSACTION_UPDATE:
        keys[i] = buildKeyName(nextKeynum(), zeropadding, orderedinserts);
        values[i] = writeallfields ? randomValues() : randomSingleValue();
        break;
      case TRANSACTION_INSERT:
        long keynum = transactioninsertkeysequence.nextValue();
        insertedKeys[inserted++] = keynum;
        keys[i] = buildKeyName(keynum, zeropadding, orderedinserts);
        values[i] = randomValues();
        break;
      default:
        break;
      }
    }

    try {
      db.transaction(table, keys, values, operations, fields, new HashMap<>());
    } finally {
      for (int i = 0; i < inserted; i++) {
        transactioninsertkeysequence.acknowledge(insertedKeys[i]);
      }
    }
  }

  private Map<String, ByteIterator> randomValues() {
    Map<String, ByteIterator> values = new HashMap<>();
    for (String field : fieldnames) {
      values.put(field, new RandomByteIterator(fieldlengthgenerator.nextValue().longValue()));
    }
    return values;
  }

  private Map<String, ByteIterator> randomSingleValue() {
    Map<String, ByteIterator> value = new HashMap<>();
    String field = fieldnames.get(fieldchooser.nextValue().intValue());
    value.put(field, new RandomByteIterator(fieldlengthgenerator.nextValue().longValue()));
    return value;
  }
}
