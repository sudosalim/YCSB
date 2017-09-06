package com.yahoo.ycsb.workloads.soe;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.soe.PredicateGenerator;
import com.yahoo.ycsb.workloads.CoreWorkload;
import com.yahoo.ycsb.WorkloadException;

import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;

/**
 * Created by oleksandr.gyryk on 3/20/17.
 */
public class SoeWorkload extends CoreWorkload {

  protected DiscreteGenerator operationchooser;

  public static final String SOE_INSERT_PROPORTION_PROPERTY = "soe_insert";
  public static final String SOE_INSERT_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_UPDATE_PROPORTION_PROPERTY = "soe_update";
  public static final String SOE_UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_READ_PROPORTION_PROPERTY = "soe_read";
  public static final String SOE_READ_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_SCAN_PROPORTION_PROPERTY = "soe_scan";
  public static final String SOE_SCAN_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_PAGE_PROPORTION_PROPERTY = "soe_page";
  public static final String SOE_PAGE_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_SEARCH_PROPORTION_PROPERTY = "soe_search";
  public static final String SOE_SEARCH_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_NESTSCAN_PROPORTION_PROPERTY = "soe_nestscan";
  public static final String SOE_NESTSCAN_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_ARRAYSCAN_PROPORTION_PROPERTY = "soe_arrayscan";
  public static final String SOE_ARRAYSCAN_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_ARRAYDEEPSCAN_PROPORTION_PROPERTY = "soe_arraydeepscan";
  public static final String SOE_ARRAYDEEPSCAN_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_REPORT_PROPORTION_PROPERTY = "soe_report";
  public static final String SOE_REPORT_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_REPORT2_PROPORTION_PROPERTY = "soe_report2";
  public static final String SOE_REPORT2_PROPORTION_PROPERTY_DEFAULT = "0.00";

  public static final String SOE_DATASETPATH_PROPORTION_PROPERTY = "soe_dataset";
  public static final String SOE_DATASETPATH_PROPORTION_PROPERTY_DEFAULT = "workloads/soe/data";

  private PredicateGenerator predicateGenerator;

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    operationchooser = createOperationGenerator(p);
    long insertstart =
        Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    predicateGenerator = new PredicateGenerator(recordcount, insertstart,
        p.getProperty(SOE_DATASETPATH_PROPORTION_PROPERTY, SOE_DATASETPATH_PROPORTION_PROPERTY_DEFAULT));
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
    case "SOE_INSERT":
      doTransactionSoeInsert(db, predicateGenerator);
      break;
    case "SOE_UPDATE":
      doTransactionSoeUpdate(db, predicateGenerator);
      break;
    case "SOE_READ":
      doTransactionSoeRead(db, predicateGenerator);
      break;
    case "SOE_SCAN":
      doTransactionSoeScan(db, predicateGenerator);
      break;
    case "SOE_PAGE":
      doTransactionSoePage(db, predicateGenerator);
      break;
    case "SOE_SEARCH":
      doTransactionSoeSearch(db, predicateGenerator);
      break;
    case "SOE_NESTSCAN":
      doTransactionSoeNestScan(db, predicateGenerator);
      break;
    case "SOE_ARRAYSCAN":
      doTransactionSoeArrayScan(db, predicateGenerator);
      break;
    case "SOE_ARRAYDEEPSCAN":
      doTransactionSoeArrayDeepScan(db, predicateGenerator);
      break;
    case "SOE_REPORT":
      doTransactionSoeReport(db, predicateGenerator);
      break;
    case "SOE_REPORT2":
      doTransactionSoeReport2(db, predicateGenerator);
      break;
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }


  public void doTransactionSoeInsert(DB db, PredicateGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.soeInsert(table, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeUpdate(DB db, PredicateGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.soeUpdate(table, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeRead(DB db, PredicateGenerator generator) {
    try {
      HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
      db.soeRead(table, cells, generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeScan(DB db, PredicateGenerator generator) {
    try {
      db.soeScan(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoePage(DB db, PredicateGenerator generator) {
    try {
      db.soePage(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeSearch(DB db, PredicateGenerator generator) {
    try {
      db.soeSearch(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeNestScan(DB db, PredicateGenerator generator) {
    try {
      db.soeNestScan(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeArrayScan(DB db, PredicateGenerator generator) {
    try {
      db.soeArrayScan(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeArrayDeepScan(DB db, PredicateGenerator generator) {
    try {
      db.soeArrayDeepScan(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeReport(DB db, PredicateGenerator generator) {
    try {
      db.soeReport(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }

  public void doTransactionSoeReport2(DB db, PredicateGenerator generator) {
    try {
      db.soeReport2(table, new Vector<HashMap<String, ByteIterator>>(), generator);
    } catch (Exception ex) {
      ex.printStackTrace();
      ex.printStackTrace(System.out);
    }
  }


  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
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


    final double soeInsert = Double.parseDouble(
        p.getProperty(SOE_INSERT_PROPORTION_PROPERTY, SOE_INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double soeUpdate = Double.parseDouble(
        p.getProperty(SOE_UPDATE_PROPORTION_PROPERTY, SOE_UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double soeRead = Double.parseDouble(
        p.getProperty(SOE_READ_PROPORTION_PROPERTY, SOE_READ_PROPORTION_PROPERTY_DEFAULT));
    final double soeScan = Double.parseDouble(
        p.getProperty(SOE_SCAN_PROPORTION_PROPERTY, SOE_SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double soePage = Double.parseDouble(
        p.getProperty(SOE_PAGE_PROPORTION_PROPERTY, SOE_PAGE_PROPORTION_PROPERTY_DEFAULT));
    final double soeSearch = Double.parseDouble(
        p.getProperty(SOE_SEARCH_PROPORTION_PROPERTY, SOE_SEARCH_PROPORTION_PROPERTY_DEFAULT));
    final double soeNetscan = Double.parseDouble(
        p.getProperty(SOE_NESTSCAN_PROPORTION_PROPERTY, SOE_NESTSCAN_PROPORTION_PROPERTY_DEFAULT));
    final double soeArrayscan = Double.parseDouble(
        p.getProperty(SOE_ARRAYSCAN_PROPORTION_PROPERTY, SOE_ARRAYSCAN_PROPORTION_PROPERTY_DEFAULT));
    final double soeArraydeepscan = Double.parseDouble(
        p.getProperty(SOE_ARRAYDEEPSCAN_PROPORTION_PROPERTY, SOE_ARRAYDEEPSCAN_PROPORTION_PROPERTY_DEFAULT));
    final double soeReport = Double.parseDouble(
        p.getProperty(SOE_REPORT_PROPORTION_PROPERTY, SOE_REPORT_PROPORTION_PROPERTY_DEFAULT));
    final double soeReport2 = Double.parseDouble(
        p.getProperty(SOE_REPORT2_PROPORTION_PROPERTY, SOE_REPORT2_PROPORTION_PROPERTY_DEFAULT));


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

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }

    if (soeInsert > 0) {
      operationchooser.addValue(soeInsert, "SOE_INSERT");
    }

    if (soeUpdate > 0) {
      operationchooser.addValue(soeUpdate, "SOE_UPDATE");
    }

    if (soeRead > 0) {
      operationchooser.addValue(soeRead, "SOE_READ");
    }

    if (soeScan > 0) {
      operationchooser.addValue(soeScan, "SOE_SCAN");
    }

    if (soePage > 0) {
      operationchooser.addValue(soePage, "SOE_PAGE");
    }

    if (soeSearch > 0) {
      operationchooser.addValue(soeSearch, "SOE_SEARCH");
    }

    if (soeNetscan > 0) {
      operationchooser.addValue(soeNetscan, "SOE_NESTSCAN");
    }

    if (soeArrayscan > 0) {
      operationchooser.addValue(soeArrayscan, "SOE_ARRAYSCAN");
    }

    if (soeArraydeepscan > 0) {
      operationchooser.addValue(soeArraydeepscan, "SOE_ARRAYDEEPSCAN");
    }

    if (soeReport > 0) {
      operationchooser.addValue(soeReport, "SOE_REPORT");
    }

    if (soeReport2 > 0) {
      operationchooser.addValue(soeReport2, "SOE_REPORT2");
    }

    return operationchooser;
  }
}

