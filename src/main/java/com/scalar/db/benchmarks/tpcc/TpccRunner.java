package com.scalar.db.benchmarks.tpcc;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.transaction.DeliveryTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.NewOrderTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.OrderStatusTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.PaymentTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.StockLevelTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.TpccTransaction;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TpccRunner {

  private final DistributedTransactionManager manager;
  private final TpccConfig config;
  private final TpccTransaction newOrder;
  private final TpccTransaction payment;
  private final TpccTransaction orderStatus;
  private final TpccTransaction delivery;
  private final TpccTransaction stockLevel;
  private final Map<Type, TpccTransaction> transactionMap;

  public TpccRunner(DistributedTransactionManager m, TpccConfig c) {
    manager = m;
    config = c;
    newOrder = new NewOrderTransaction(c);
    payment = new PaymentTransaction(c);
    orderStatus = new OrderStatusTransaction(c);
    delivery = new DeliveryTransaction(c);
    stockLevel = new StockLevelTransaction(c);
    transactionMap =
        ImmutableMap.<Type, TpccTransaction>builder()
            .put(Type.NEW_ORDER, newOrder)
            .put(Type.PAYMENT, payment)
            .put(Type.ORDER_STATUS, orderStatus)
            .put(Type.DELIVERY, delivery)
            .put(Type.STOCK_LEVEL, stockLevel)
            .build();
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
    } else if (x
        <= config.getRateNewOrder() + config.getRatePayment() + config.getRateOrderStatus()) {
      return Type.ORDER_STATUS;
    } else if (x
        <= config.getRateNewOrder()
            + config.getRatePayment()
            + config.getRateOrderStatus()
            + config.getRateDelivery()) {
      return Type.DELIVERY;
    } else {
      return Type.STOCK_LEVEL;
    }
  }

  /** Runs a TPC-C transaction. */
  public void run(AtomicBoolean isRunning, AtomicInteger errorCounter) throws TransactionException {
    Type type = decideType();
    TpccTransaction tx = transactionMap.get(type);
    tx.generate();
    while (isRunning.get()) {
      try {
        tx.execute(manager);
        break;
      } catch (CrudConflictException | CommitConflictException e) {
        errorCounter.incrementAndGet();
        Uninterruptibles.sleepUninterruptibly(config.getBackoff(), TimeUnit.MILLISECONDS);
      }
    }
  }

  /** Runs a TPC-C transaction without retrying. */
  public void run() throws TransactionException {
    Type type = decideType();
    TpccTransaction tx = transactionMap.get(type);
    tx.generate();
    tx.execute(manager);
  }
}
