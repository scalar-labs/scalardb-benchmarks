package com.scalar.db.benchmarks.tpcc;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.transaction.NewOrderTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.PaymentTransaction;
import com.scalar.db.exception.transaction.TransactionException;

public class TpccRunner {
  private final DistributedTransactionManager manager;
  private final TpccConfig config;
  private final NewOrderTransaction newOrder = new NewOrderTransaction();
  private final PaymentTransaction payment = new PaymentTransaction();

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
    switch (type) {
      case PAYMENT:
        payment.generate(config.getNumWarehouse());
        payment.execute(manager);
        break;
      case NEW_ORDER:
        newOrder.generate(config.getNumWarehouse());
        newOrder.execute(manager);
        break;
      default:
        break;
    }
  }
}
