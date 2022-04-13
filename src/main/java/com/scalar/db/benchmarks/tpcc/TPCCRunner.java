package com.scalar.db.benchmarks.tpcc;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.transaction.NewOrderTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.PaymentTransaction;
import com.scalar.db.exception.transaction.TransactionException;

public class TPCCRunner {
  private final DistributedTransactionManager manager;
  private final TPCCConfig config;
  private final NewOrderTransaction newOrder = new NewOrderTransaction();
  private final PaymentTransaction payment = new PaymentTransaction();

  public TPCCRunner(DistributedTransactionManager m, TPCCConfig c) {
    manager = m;
    config = c;
  }

  public enum Type {
    None,
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
  }

  public Type decideType() {
    int x = TPCCUtil.randomInt(1, 100);
    if (x <= config.getRatePayment()) {
      return Type.Payment;
    } else {
      return Type.NewOrder;
    }
  }

  public void run() throws TransactionException {
    Type type = decideType();
    switch (type) {
      case Payment:
        payment.generate(config.getNumWarehouse());
        payment.execute(manager);
        break;
      case NewOrder:
        newOrder.generate(config.getNumWarehouse());
        newOrder.execute(manager);
        break;
      default:
        break;
    }
    return;
  }
}
