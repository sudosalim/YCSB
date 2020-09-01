package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.*;

/**
 *
 */
public class CustomTransactionWorkload extends CoreWorkload {

  public static final String TRANSACTION_READSPROPORTION_PROPERTY = "transactionreadproportion";
  public static final String TRANSACTION_READSPROPORTION_PROPERTY_DEFAULT = "0.5";

  public static final String TRANSACTION_WRITESPROPORTION_PROPERTY = "transactionupdateproportion";
  public static final String TRANSACTION_WRITESPROPORTION_PROPERTY_DEFAULT = "0.5";

  public static final String TRANSACTION_INSERTSPROPORTION_PROPERTY = "transactioninsertproportion";
  public static final String TRANSACTION_INSERTSPROPORTION_PROPERTY_DEFAULT = "0.0";

  public static final String DOCUMENTS_IN_TRANSACTION_PROPERTY = "documentsintransaction";
  public static final String DOCUMENTS_IN_TRANSACTION_PROPERTY_DEFAULT = "4";

  public static final String TRANSACTION_PROPORTION_PROPERTY = "transactionproportion";
  public static final String TRANSACTION_PROPORTION_PROPERTY_DEFAULT = "0";

  protected DiscreteGenerator transactionoperationchooser;
  private int documentsintransaction;
  private double transactionreadsproportion;
  private double transactionwritesproportion;
  private double transactioninsertsproportion;
  private List<String> fieldnames;
  private Measurements measurements = Measurements.getMeasurements();
  protected DiscreteGenerator operationchooser;


  private HashMap<String, ByteIterator> buildRandomValues() {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      values.put(fieldkey, data);
    }
    return values;
  }

  private HashMap<String, ByteIterator> buildRandomSingleValue() {
    HashMap<String, ByteIterator> value = new HashMap<>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    value.put(fieldkey, data);
    return value;
  }


  @Override
  public void init(Properties p) throws WorkloadException {
    documentsintransaction = Integer.parseInt(p.getProperty(DOCUMENTS_IN_TRANSACTION_PROPERTY,
        DOCUMENTS_IN_TRANSACTION_PROPERTY_DEFAULT));
    transactionreadsproportion = Double.parseDouble(p.getProperty(TRANSACTION_READSPROPORTION_PROPERTY,
        TRANSACTION_READSPROPORTION_PROPERTY_DEFAULT));
    transactionwritesproportion = Double.parseDouble(p.getProperty(TRANSACTION_WRITESPROPORTION_PROPERTY,
        TRANSACTION_WRITESPROPORTION_PROPERTY_DEFAULT));
    transactioninsertsproportion = Double.parseDouble(p.getProperty(TRANSACTION_INSERTSPROPORTION_PROPERTY,
        TRANSACTION_INSERTSPROPORTION_PROPERTY_DEFAULT));
    transactionoperationchooser = createTransactionOperationGenerator(transactionreadsproportion,
        transactionwritesproportion, transactioninsertsproportion);
    operationchooser = createOperationGenerator(p);


    fieldcount =
        Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    fieldnames = new ArrayList<>();
    for (int i = 0; i < fieldcount; i++) {
      fieldnames.add("field" + i);
    }

    super.init(p);
  }


  public void doTransactionCustom(DB db) {

    String[] transationKeys = new String[documentsintransaction];
    HashMap<String, ByteIterator>[] transationValues = new HashMap[documentsintransaction];
    String[] transationOperations = new String[documentsintransaction];
    HashSet<String> fields = null;
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();

    //String transactionOperation = transactionoperationchooser.nextString();

    for(int i=0; i<documentsintransaction; i++){
      transationOperations[i] = transactionoperationchooser.nextString();

      switch (transationOperations[i]) {
      case "TRREAD":
        transationKeys[i] = buildKeyName(nextKeynum());
        transationValues[i] = null;
        if (!readallfields) {
          String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
          fields = new HashSet<>();
          fields.add(fieldname);
        }
        break;
      case "TRUPDATE":
        transationKeys[i] = buildKeyName(nextKeynum());
        if (writeallfields) {
          transationValues[i] = buildRandomValues();
        } else {
          transationValues[i] = buildRandomSingleValue();
        }
        break;
      case "TRINSERT":
        int keynum = keysequence.nextValue().intValue();
        transationKeys[i] = buildKeyName(keynum);
        if (writeallfields) {
          transationValues[i] = buildRandomValues();
        } else {
          transationValues[i] = buildRandomSingleValue();
        }
        break;
      default:
        break;
      }
    }

    db.transaction(table, transationKeys, transationValues, transationOperations, fields, result);
  }


  @Override
  public boolean doTransaction(DB db, Object threadstate) {

    String operation = operationchooser.nextString();
    if(operation == null) {
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
      doTransactionCustom(db);
      break;
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }



  protected static DiscreteGenerator createTransactionOperationGenerator(final double transactionreadsproportion,
                                                                         final double transactionwritesproportion,
                                                                         final double transactioninsertsproportion) {

    final DiscreteGenerator operationchooser = new DiscreteGenerator();


    if (transactionreadsproportion > 0) {
      operationchooser.addValue(transactionreadsproportion, "TRREAD");
    }

    if (transactionwritesproportion > 0) {
      operationchooser.addValue(transactionwritesproportion, "TRUPDATE");
    }

    if (transactioninsertsproportion > 0) {
      operationchooser.addValue(transactioninsertsproportion, "TRINSERT");
    }

    return operationchooser;
  }


  protected static DiscreteGenerator createOperationGenerator(final Properties p) {


    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
    final double transactionproportion = Double.parseDouble(
        p.getProperty(TRANSACTION_PROPORTION_PROPERTY, TRANSACTION_PROPORTION_PROPERTY_DEFAULT));


    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (scanproportion > 0) {
      operationchooser.addValue(scanproportion, "SCAN");
    }


    if (transactionproportion > 0) {
      operationchooser.addValue(transactionproportion, "TRANSACTION");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }

    return operationchooser;
  }

}