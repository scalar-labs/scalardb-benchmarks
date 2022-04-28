package com.scalar.db.benchmarks.tpcc;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.transaction.NewOrderTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.PaymentTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.TpccTransaction;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TpccRunner {
  private final DistributedTransactionManager manager;
  private final TpccConfig config;
  private final TpccTransaction newOrder = new NewOrderTransaction();
  private final TpccTransaction payment = new PaymentTransaction();
  private final Map<Type, TpccTransaction> transactionMap =
      ImmutableMap.<Type, TpccTransaction>builder()
      .put(Type.NEW_ORDER, newOrder)
      .put(Type.PAYMENT, payment)
      .build();

  public TpccRunner(DistributedTransactionManager m, TpccConfig c) {
    manager = m;
    config = c;
  }

  public enum Type {
    NONE,
    NEW_ORDER,
    PAYMENT,
    ORDER_STATUS,
    DELIVERY,
    STOCK_LEVEL,
  }

  private Type decideType() {
    int x = TpccUtil.randomInt(1, 100);
    if (x <= config.getRateNewOrder()) {
      return Type.NEW_ORDER;
    } else if (x <= config.getRateNewOrder() + config.getRatePayment()) {
      return Type.PAYMENT;
    } else if (x <= config.getRateNewOrder() + config.getRatePayment()
        + config.getRateOrderStatus()) {
      return Type.ORDER_STATUS;
    } else if (x <= config.getRateNewOrder() + config.getRatePayment()
        + config.getRateOrderStatus() + config.getRateDelivery()) {
      return Type.DELIVERY;
    } else {
      return Type.STOCK_LEVEL;
    }
  }

  /**
   * Runs a TPC-C transaction.
   */
  public void run() throws TransactionException {
    Type type = decideType();
    TpccTransaction tx = transactionMap.get(type);
    tx.generate(config.getNumWarehouse());
    while (true) {
      try {
        tx.execute(manager);
        break;
      } catch (CrudConflictException | CommitConflictException e) {
        Uninterruptibles.sleepUninterruptibly(config.getBackoff(), TimeUnit.MILLISECONDS);
      }
    }
  }
}
