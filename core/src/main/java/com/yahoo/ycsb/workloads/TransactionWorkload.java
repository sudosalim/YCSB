package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.*;

public class TransactionWorkload extends CoreWorkload {

  public static final String OPERATIONS_IN_TRANSACTION_PROPERTY = "operationsintransaction";
  public static final String OPERATIONS_IN_TRANSACTION_PROPERTY_DEFAULT = "4";

  public static final String DOCUMENTS_IN_TRANSACTION_PROPERTY = "documentsintransaction";
  public static final String DOCUMENTS_IN_TRANSACTION_PROPERTY_DEFAULT = "4";

  private int operationsintransaction;
  private int documentsintransaction;
  private List<String> fieldnames;
  private Measurements measurements = Measurements.getMeasurements();


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
    operationsintransaction = Integer.parseInt(p.getProperty(OPERATIONS_IN_TRANSACTION_PROPERTY,
        OPERATIONS_IN_TRANSACTION_PROPERTY_DEFAULT));
    documentsintransaction = Integer.parseInt(p.getProperty(DOCUMENTS_IN_TRANSACTION_PROPERTY,
        DOCUMENTS_IN_TRANSACTION_PROPERTY_DEFAULT));
    super.init(p);
  }


  @Override
  public boolean doTransaction(DB db, Object threadstate) {


    ArrayList<String> transactionKeys = new ArrayList<>();
    for (int i = 0; i < documentsintransaction; i++) {
      long keynum = nextKeynum();
      transactionKeys.add(buildKeyName(keynum));
    }

    ArrayList<String> operations = new ArrayList<>();
    for (int i = 0; i < operationsintransaction; i++) {
      operations.add(operationchooser.nextString());
    }


    HashSet<String> fields = null;
    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
      fields = new HashSet<String>();
      fields.add(fieldname);
    }

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      values = buildRandomValues();
    } else {
      values = buildRandomSingleValue();
    }


    long ist = measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();

    db.transaction(table, transactionKeys.stream().toArray(String[]::new),operations.stream().toArray(String[]::new),
        fields, values);

    long en = System.nanoTime();
    measurements.measure("TRANSACTION", (int) ((en - st) / 1000));
    measurements.measureIntended("TRANSACTION", (int) ((en - ist) / 1000));

    return true;
  }

}
