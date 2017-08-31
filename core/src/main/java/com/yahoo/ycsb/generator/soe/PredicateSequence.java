package com.yahoo.ycsb.generator.soe;

/**
 * Created by oleksandr.gyryk on 8/30/17.
 */
public class PredicateSequence {

  private PredicateSequence predicate;
  private String pname;
  private String pvalueA;
  private String pvalueB;

  public void setNestedPredicate(PredicateSequence nestedPredicate) {
    predicate = nestedPredicate;
  }

  public PredicateSequence getNestedPredicate() {
    return predicate;
  }

  public void setName(String name) {
    pname = name;
  }

  public String getName() {
    return pname;
  }

  public void setValueA(String value) {
    pvalueA = value;
  }

  public String getValueA(){
    return pvalueA;
  }

  public void setValueB(String value) {
    pvalueB = value;
  }

  public String getValueB(){
    return pvalueB;
  }
}
