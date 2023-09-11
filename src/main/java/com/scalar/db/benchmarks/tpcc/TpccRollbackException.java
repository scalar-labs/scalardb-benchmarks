package com.scalar.db.benchmarks.tpcc;

public class TpccRollbackException extends Exception {

  public TpccRollbackException(String message) {
    super(message);
  }
}
