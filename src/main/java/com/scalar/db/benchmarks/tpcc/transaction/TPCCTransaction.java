package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;

public interface TPCCTransaction {
  public void generate(int numWarehouse);

  public void execute(DistributedTransactionManager manager) throws TransactionException;
}
