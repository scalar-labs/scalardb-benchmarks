package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.benchmarks.tpcc.TpccConfig;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.benchmarks.tpcc.table.Customer;
import com.scalar.db.benchmarks.tpcc.table.Order;
import com.scalar.db.benchmarks.tpcc.table.OrderLine;
import com.scalar.db.benchmarks.tpcc.table.OrderSecondary;
import com.scalar.db.benchmarks.tpcc.table.Warehouse;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class OrderStatusTransaction implements TpccTransaction {

  private final TpccConfig config;
  private int warehouseId;
  private int districtId;
  private int customerId;
  private boolean byLastName;
  private String lastName;

  public OrderStatusTransaction(TpccConfig c) {
    config = c;
  }

  private int getOrderIdBySecondaryIndex(DistributedTransaction tx) throws TransactionException {
    List<Result> results = tx.scan(Order.createScan(warehouseId, districtId, customerId));
    if (results.size() < 1) {
      throw new TransactionException("Invalid scan on order-secondary");
    }
    results.sort(Order.ORDER_ID_COMPARATOR);
    return results.get(0).getValue(Order.KEY_ID).get().getAsInt();
  }

  private int getOrderIdByTableIndex(DistributedTransaction tx) throws TransactionException {
    List<Result> results = tx.scan(OrderSecondary.createScan(warehouseId, districtId, customerId));
    if (results.size() != 1) {
      throw new TransactionException("Invalid scan on order-secondary");
    }
    return results.get(0).getValue(OrderSecondary.KEY_ORDER_ID).get().getAsInt();
  }

  /**
   * Generates arguments for the order-status transaction.
   */
  @Override
  public void generate() {
    int numWarehouse = config.getNumWarehouse();
    warehouseId = TpccUtil.randomInt(1, numWarehouse);
    districtId = TpccUtil.randomInt(1, Warehouse.DISTRICTS);
    byLastName = TpccUtil.randomInt(1, 100) <= 60;
    if (byLastName) {
      customerId = Customer.UNUSED_ID;
      lastName = TpccUtil.getNonUniformRandomLastNameForRun();
    } else {
      customerId = TpccUtil.getCustomerId();
    }
  }

  /**
   * Executes the order-status transaction.
   *
   * @param manager a {@code DistributedTransactionManager} object
   */
  @Override
  public void execute(DistributedTransactionManager manager) throws TransactionException {
    DistributedTransaction tx = manager.start();

    try {
      if (byLastName) {
        if (config.useTableIndex()) {
          customerId = TpccUtil.getCustomerIdByTableIndex(
              tx, warehouseId, districtId, lastName);
        } else {
          customerId = TpccUtil.getCustomerIdBySecondaryIndex(
              tx, warehouseId, districtId, lastName);
        }
      }

      // Get customer
      Optional<Result> result = tx.get(Customer.createGet(warehouseId, districtId, customerId));
      if (!result.isPresent()) {
        throw new TransactionException("Customer not found");
      }

      // Find the last order of the customer
      int orderId;
      if (config.useTableIndex()) {
        orderId = getOrderIdByTableIndex(tx);
      } else {
        orderId = getOrderIdBySecondaryIndex(tx);
      }

      // Get order
      result = tx.get(Order.createGet(warehouseId, districtId, orderId));
      if (!result.isPresent()) {
        throw new TransactionException("Order not found");
      }

      // Get order-line
      List<Result> orderLines = tx.scan(OrderLine.createScan(warehouseId, districtId, orderId));
      orderLines.forEach(line -> {
        int supplyWarehouseId = line.getValue(OrderLine.KEY_SUPPLY_W_ID).get().getAsInt();
        int itemId = line.getValue(OrderLine.KEY_ITEM_ID).get().getAsInt();
        int quantity = line.getValue(OrderLine.KEY_QUANTITY).get().getAsInt();
        double amount = line.getValue(OrderLine.KEY_AMOUNT).get().getAsDouble();
        Date deliveryDate = new Date(line.getValue(OrderLine.KEY_DELIVERY_D).get().getAsLong());
      });

      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }
}
