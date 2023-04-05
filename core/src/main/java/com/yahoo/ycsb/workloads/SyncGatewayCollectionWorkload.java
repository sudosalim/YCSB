package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.Properties;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.SequentialGenerator;

/**
 * This class overrides the behaviour of the current implementation of the
 * collection and scope sequential generator not to depend on the {insertstart}.
 * This implies the following behaviour:
 * 
 * - For each worker instance, documents are processed on all collections in a
 * given range, this way all worker machines will interact with all collections
 * on assigned workload range as given by the flags.
 * 
 * - This means load phase can use multiple clients and distribute workloads
 * across them.
 */
public class SyncGatewayCollectionWorkload extends CustomCollectionWorkload {

    private static final String IGNORE = "IGNORE";

    /**
     * Initialize the scenario.
     * Called once, in the main client thread, before any operations are started.
     */
    public void init(Properties p) throws WorkloadException {
        super.init(p);
        // Override the behaviour of the collection/ scope sequential generator.
        this.collectionchooser = new SequentialGenerator(0, this.collectioncount - 1);
        this.scopechooser = new SequentialGenerator(0, this.scopes.length - 1);

        // Trying to stop grant_access and init_user from doing any operations, prevents
        // operations from hanging. Need to be refactored in the future
        boolean grantAccess = p.getProperty("syncgateway.grantaccesstoall", "false").equals("true");
        boolean initUsers = p.getProperty("syncgateway.initusers", "false").equals("true");
        if (grantAccess || initUsers) {
            this.operationchooser = new DiscreteGenerator();
            this.operationchooser.addValue(1, IGNORE);
        }
    }

    public void doTransactionInsert(DB db) {
        // choose the next key
        long keynum = this.transactioninsertkeysequence.nextValue() + this.sgInserstart;

        int collnum = (int) this.nextcollectionNum();
        String collname = this.collections[collnum];

        int scopenum = (int) this.nextscopeNum();
        String scopename = this.scopes[scopenum];

        try {
            String dbkey = this.buildKeyName(keynum);

            HashMap<String, ByteIterator> values = this.buildValues(dbkey);
            db.insert(table, dbkey, values, scopename, collname);
        } finally {
            this.transactioninsertkeysequence.acknowledge(keynum);
        }
    }

    @Override
    public boolean doInsertCollection(DB db, Object threadstate, String scope, String collection, long key) {

        long keynum = this.sgInserstart + key;

        String dbkey = buildKeyName(keynum);

        Status status;
        int numOfRetries = 0;
        do {
            HashMap<String, ByteIterator> values = buildValues(dbkey);

            status = db.insert(table, dbkey, values, scope, collection);
            if (null != status && status.isOk()) {
                break;
            }
            // Retry if configured. Without retrying, the load process will fail
            // even if one single insertion fails. User can optionally configure
            // an insertion retry limit (default is 0) to enable retry.
            if (++numOfRetries <= insertionRetryLimit) {
                System.err.println("Retrying insertion, retry count: " + numOfRetries);
                try {
                    // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
                    int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    break;
                }

            } else {
                System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
                        "Insertion Retry Limit: " + insertionRetryLimit);
                break;

            }
        } while (true);

        return null != status && status.isOk();
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        String operation = operationchooser.nextString();
        if (operation == null || operation.equals(IGNORE)) {
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
            default:
                doTransactionReadModifyWrite(db);
        }
        return true;
    }

}
