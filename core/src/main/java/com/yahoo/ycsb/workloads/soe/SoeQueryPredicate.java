package com.yahoo.ycsb.workloads.soe;

/**
 * Created by oleksandr.gyryk on 3/20/17.
 */
public class SoeQueryPredicate {

  public static final String SOE_PREDICATE_TYPE_STRING = "string";
  public static final String SOE_PREDICATE_TYPE_INTEGER = "int";
  public static final String SOE_PREDICATE_TYPE_BOOLEAN = "bool";


  private String name;
  private String valueA;
  private String valueB;
  private String docid;
  private String operation;
  private String relation;
  private String type = SOE_PREDICATE_TYPE_STRING;
  private SoeQueryPredicate nestedPredicateA = null;
  private SoeQueryPredicate nestedPredicateB = null;
  private SoeQueryPredicate nestedPredicateC = null;
  private SoeQueryPredicate nestedPredicateD = null;


  public void setName(String param) {
    this.name = param;
  }

  public void setValueA(String param) {
    this.valueA = param;
  }

  public void setValueB(String param) {
    this.valueB = param;
  }

  public void setDocid(String param) {
    this.docid = param;
  }

  public void setOperation(String param) {
    this.operation = param;
  }

  public void setRelation(String param) {
    this.relation = param;
  }

  public void setType(String param) {
    this.type = param;
  }

  public void setNestedPredicateA(SoeQueryPredicate param) {
    this.nestedPredicateA = param;
  }

  public void setNestedPredicateB(SoeQueryPredicate param) {
    this.nestedPredicateB = param;
  }

  public void setNestedPredicateC(SoeQueryPredicate param) {
    this.nestedPredicateC = param;
  }

  public void setNestedPredicateD(SoeQueryPredicate param) {
    this.nestedPredicateD = param;
  }

  public String getName() {
    return name;
  }

  public String getValueA() {
    return valueA;
  }

  public String getValueB() {
    return valueB;
  }

  public String getDocid() {
    return docid;
  }

  public String getOperation() {
    return operation;
  }

  public String getRelation() {
    return relation;
  }

  public String getType() {
    return type;
  }

  public SoeQueryPredicate getNestedPredicateA() {
    return nestedPredicateA;
  }

  public SoeQueryPredicate getNestedPredicateB() {
    return nestedPredicateB;
  }

  public SoeQueryPredicate getNestedPredicateC() {
    return nestedPredicateC;
  }

  public SoeQueryPredicate getNestedPredicateD() {
    return nestedPredicateD;
  }
}