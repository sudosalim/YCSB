package com.yahoo.ycsb.generator.soe;

import com.yahoo.ycsb.generator.UniformLongGenerator;

import java.util.Random;

/**
 * Created by oleksandr.gyryk on 8/30/17.
 */
public class PredicateGenerator  {

  private UniformLongGenerator uniformDocIDgen;
  private Random rnd = new Random();


  public PredicateGenerator(long totalDocs, String dataPath){
    ValuesContainer.init(dataPath);
    uniformDocIDgen = new UniformLongGenerator(0, totalDocs);
  }

  public String getDocid(){
    return ValuesContainer.DEFAULTS_CUSTOMERDOCNAME_PREFIX + uniformDocIDgen.nextValue();
  }

  public String getRandomDocument(){
    return pick(ValuesContainer.getData().get(ValuesContainer.DOCUMENT));
  }

  public PredicateSequence getPagePredicateSequence(){
    PredicateSequence predicateSequense = new PredicateSequence();
    predicateSequense.setName(ValuesContainer.OBJ_ADDRES_FLD_ZIP);
    predicateSequense.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRES_FLD_ZIP)));
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
    predicateSequense.setName(ValuesContainer.OBJ_ADDRESS_OBJ_PREVADDRES_FLD_ZIP);
    predicateSequense.setValueA(pick(
        ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRESS_OBJ_PREVADDRES_FLD_ZIP)));
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
    PredicateSequence objVisitedplacesFldCity = new PredicateSequence();

    objVisitedplacesFldCountry.setName(ValuesContainer.OBJ_VISITEDPLACES_FLD_COUNTRY);
    objVisitedplacesFldCountry.setValueA(pick(
        ValuesContainer.getData().get(ValuesContainer.OBJ_VISITEDPLACES_FLD_COUNTRY)));

    objVisitedplacesFldCity.setName(ValuesContainer.OBJ_VISITEDPLACES_FLD_CITIES);
    objVisitedplacesFldCity.setValueA(pick(
        ValuesContainer.getData().get(ValuesContainer.OBJ_VISITEDPLACES_FLD_CITIES)));

    objVisitedplacesFldCountry.setNestedPredicate(objVisitedplacesFldCity);

    return objVisitedplacesFldCountry;
  }

  public PredicateSequence getReport1PrediateSequence(){
    PredicateSequence predicateSequense = new PredicateSequence();
    predicateSequense.setName(ValuesContainer.OBJ_ADDRES_FLD_ZIP);
    predicateSequense.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRES_FLD_ZIP)));

    PredicateSequence orderList = new PredicateSequence();
    orderList.setName(ValuesContainer.FLD_ORDERLIST);

    predicateSequense.setNestedPredicate(orderList);
    return predicateSequense;
  }

  public PredicateSequence getReport2PrediateSequence(){

    PredicateSequence objAddressFldZip = new PredicateSequence();
    PredicateSequence fldOrderMonth = new PredicateSequence();
    PredicateSequence fldOrderSaleprice = new PredicateSequence();
    PredicateSequence orderList = new PredicateSequence();

    objAddressFldZip.setName(ValuesContainer.OBJ_ADDRES_FLD_ZIP);
    objAddressFldZip.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.OBJ_ADDRES_FLD_ZIP)));

    fldOrderMonth.setName(ValuesContainer.FLD_ORDERMONTH);
    fldOrderMonth.setValueA(pick(ValuesContainer.getData().get(ValuesContainer.FLD_ORDERMONTH)));

    fldOrderSaleprice.setName(ValuesContainer.FLD_ORDERSALEPRICE);
    orderList.setName(ValuesContainer.FLD_ORDERLIST);

    fldOrderSaleprice.setNestedPredicate(orderList);
    fldOrderMonth.setNestedPredicate(fldOrderSaleprice);
    objAddressFldZip.setNestedPredicate(fldOrderMonth);

    return objAddressFldZip;
  }

  private String pick(String[] source) {
    return source[rnd.nextInt(source.length)];
  }

}
