package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.exception.transaction.TransactionException;

public interface TpccTransaction {

  void execute() throws TransactionException;

  void commit() throws TransactionException;

  void abort() throws TransactionException;
}
