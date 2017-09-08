package com.yahoo.ycsb.generator.soe;

import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.UniformLongGenerator;

import java.util.Random;

/**
 * Created by oleksandr.gyryk on 8/30/17.
 */
public class PredicateGenerator  {

  private UniformLongGenerator uniformDocIdGen;
  private Random rnd = new Random();
  private CounterGenerator sequentialDocIdGen;

  public PredicateGenerator(long totalDocs, long insertstart, String dataPath){
    ValuesContainer.init(dataPath);
    uniformDocIdGen = new UniformLongGenerator(0, totalDocs - 1);
    sequentialDocIdGen = new CounterGenerator(insertstart + 1);
  }

  public String getRandomDocId(){
    return ValuesContainer.DEFAULTS_CUSTOMERDOCNAME_PREFIX + uniformDocIdGen.nextValue();
  }

  public String getRandomDocument(){
    return pick(ValuesContainer.getData().get(ValuesContainer.DOCUMENT));
  }

  public String getSequentialDocId(){
    return ValuesContainer.DEFAULTS_CUSTOMERDOCNAME_PREFIX + sequentialDocIdGen.nextValue();
  }

  public PredicateSequence getPagePredicateSequence(){
    PredicateSequence predicateSequense = new PredicateSequence();
    predicateSequense.setName(ValuesContainer.OBJ_ADDRES_FLD_COUNTRY);
    predicateSequense.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRES_FLD_COUNTRY)));
    return predicateSequense;
  }

  public PredicateSequence getSearchPredicateSequnce(){
    PredicateSequence objAddressFldCountry = new PredicateSequence();
    PredicateSequence fldAgegroup = new PredicateSequence();
    PredicateSequence fldDobyear = new PredicateSequence();

    objAddressFldCountry.setName(ValuesContainer.OBJ_ADDRES_FLD_COUNTRY);
    objAddressFldCountry.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRES_FLD_COUNTRY)));

    fldAgegroup.setName(ValuesContainer.FLD_AGEGROUP);
    fldAgegroup.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.FLD_AGEGROUP)));

    fldDobyear.setName(ValuesContainer.FLD_DOBYEAR);
    fldDobyear.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.FLD_DOBYEAR)));

    fldAgegroup.setNestedPredicate(fldDobyear);
    objAddressFldCountry.setNestedPredicate(fldAgegroup);

    return objAddressFldCountry;
  }

  public PredicateSequence getNestScanPredicateSequence(){
    PredicateSequence predicateSequense = new PredicateSequence();
    predicateSequense.setName(ValuesContainer.OBJ_ADDRESS_OBJ_PREVADDRES_FLD_COUNTRY);
    predicateSequense.setValueA(pick(
        ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRESS_OBJ_PREVADDRES_FLD_COUNTRY)));
    return predicateSequense;
  }

  public PredicateSequence getArrayScanPredicateSequence(){
    PredicateSequence predicateSequense = new PredicateSequence();
    predicateSequense.setName(ValuesContainer.FLD_DEVICES);
    predicateSequense.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.FLD_DEVICES)));
    return predicateSequense;
  }

  public PredicateSequence getArrayDeepScanPredicateSequence(){
    PredicateSequence objVisitedplacesFldCountry = new PredicateSequence();
    PredicateSequence objVisitedplacesFldActivity = new PredicateSequence();

    objVisitedplacesFldCountry.setName(ValuesContainer.OBJ_VISITEDPLACES_FLD_COUNTRY);
    objVisitedplacesFldCountry.setValueA(pick(
        ValuesContainer.getData().get(ValuesContainer.OBJ_VISITEDPLACES_FLD_COUNTRY)));

    objVisitedplacesFldActivity.setName(ValuesContainer.OBJ_VISITEDPLACES_FLD_ACTIVITIES);
    objVisitedplacesFldActivity.setValueA(pick(
        ValuesContainer.getData().get(ValuesContainer.OBJ_VISITEDPLACES_FLD_ACTIVITIES)));

    objVisitedplacesFldCountry.setNestedPredicate(objVisitedplacesFldActivity);

    return objVisitedplacesFldCountry;
  }

  public PredicateSequence getReport1PrediateSequence(){
    PredicateSequence predicateSequense = new PredicateSequence();
    predicateSequense.setName(ValuesContainer.OBJ_ADDRES_FLD_COUNTRY);
    predicateSequense.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRES_FLD_COUNTRY)));

    PredicateSequence orderList = new PredicateSequence();
    orderList.setName(ValuesContainer.FLD_ORDERLIST);

    predicateSequense.setNestedPredicate(orderList);
    return predicateSequense;
  }

  public PredicateSequence getReport2PrediateSequence(){

    PredicateSequence objAddressFldCountry = new PredicateSequence();
    PredicateSequence fldOrderMonth = new PredicateSequence();
    PredicateSequence fldOrderSaleprice = new PredicateSequence();
    PredicateSequence orderList = new PredicateSequence();

    objAddressFldCountry.setName(ValuesContainer.OBJ_ADDRES_FLD_COUNTRY);
    objAddressFldCountry.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRES_FLD_COUNTRY)));

    fldOrderMonth.setName(ValuesContainer.FLD_ORDERMONTH);
    fldOrderMonth.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.FLD_ORDERMONTH)));

    fldOrderSaleprice.setName(ValuesContainer.FLD_ORDERSALEPRICE);
    orderList.setName(ValuesContainer.FLD_ORDERLIST);

    fldOrderSaleprice.setNestedPredicate(orderList);
    fldOrderMonth.setNestedPredicate(fldOrderSaleprice);
    objAddressFldCountry.setNestedPredicate(fldOrderMonth);

    return objAddressFldCountry;
  }

  private String pick(String[] source) {
    if (source.length == 0) {
      System.err.println("SOE dataset is empty. Cannot run the query.");
      System.exit(1);
    }
    return source[rnd.nextInt(source.length)];
  }

}
