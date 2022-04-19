package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;

public interface TpccRecord {
  public void insert(DistributedTransactionManager manager) throws TransactionException;
}
