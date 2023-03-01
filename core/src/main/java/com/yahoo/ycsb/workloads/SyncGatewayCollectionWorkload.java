package com.yahoo.ycsb.workloads;

import java.util.Properties;

import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.SequentialGenerator;

/**
 * This class overrides the behaviour of the current implementation of the collection
 * and scope sequential generator not to depend on the {insertstart}. This implies the
 * following behaviour:
 * 
 *  - For each worker instance, documents are processed on all collections in a given 
 * range, this way all worker machines will interact with all collections on assigned
 * workload range as given by the flags.
 * 
 *  - This means load phase can use multiple clients and distribute workloads across
 * them.
 */
public class SyncGatewayCollectionWorkload extends CustomCollectionWorkload{

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    // Override the behaviour of the collection/ scope sequential generator.
    // This class 
    this.collectionchooser = new SequentialGenerator(0,this.collectioncount -1);
    this.scopechooser = new SequentialGenerator(0,this.scopes.length -1);
  }
    
}
