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
  private final DistributedTransactionManager manager;
  private DistributedTransaction transaction;
  private int warehouseId;
  private int districtId;
  private int customerId;
  private boolean byLastName;
  private String lastName;

  public OrderStatusTransaction(DistributedTransactionManager manager, TpccConfig config) {
    this.manager = manager;
    this.config = config;
    generate();
  }

  private int getOrderIdBySecondaryIndex(DistributedTransaction tx) throws TransactionException {
    List<Result> results = tx.scan(Order.createScan(warehouseId, districtId, customerId));
    if (results.size() < 1) {
      throw new IllegalStateException(
          String.format(
              "Invalid scan on order-secondary: warehouse %d, district: %d, customer: %d",
              warehouseId, districtId, customerId));
    }
    results.sort(Order.ORDER_ID_COMPARATOR);
    return results.get(0).getValue(Order.KEY_ID).get().getAsInt();
  }

  private int getOrderIdByTableIndex(DistributedTransaction tx) throws TransactionException {
    List<Result> results = tx.scan(OrderSecondary.createScan(warehouseId, districtId, customerId));
    if (results.size() != 1) {
      throw new IllegalStateException(
          String.format(
              "Invalid scan on order-secondary: warehouse %d, district: %d, customer: %d",
              warehouseId, districtId, customerId));
    }
    return results.get(0).getValue(OrderSecondary.KEY_ORDER_ID).get().getAsInt();
  }

  private void generate() {
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

  @Override
  public void execute() throws TransactionException {
    transaction = manager.start();

    if (byLastName) {
      if (config.useTableIndex()) {
        customerId =
            TpccUtil.getCustomerIdByTableIndex(transaction, warehouseId, districtId, lastName);
      } else {
        customerId =
            TpccUtil.getCustomerIdBySecondaryIndex(transaction, warehouseId, districtId, lastName);
      }
    }

    // Get customer
    Optional<Result> result =
        transaction.get(Customer.createGet(warehouseId, districtId, customerId));
    if (!result.isPresent()) {
      throw new IllegalStateException(
          String.format(
              "Customer not found: warehouse %d, district: %d, customer: %d",
              warehouseId, districtId, customerId));
    }

    // Find the last order of the customer
    int orderId;
    if (config.useTableIndex()) {
      orderId = getOrderIdByTableIndex(transaction);
    } else {
      orderId = getOrderIdBySecondaryIndex(transaction);
    }

    // Get order
    result = transaction.get(Order.createGet(warehouseId, districtId, orderId));
    if (!result.isPresent()) {
      throw new IllegalStateException(
          String.format(
              "Order not found: warehouse %d, district: %d, order: %d",
              warehouseId, districtId, orderId));
    }

    // Get order-line
    List<Result> orderLines =
        transaction.scan(OrderLine.createScan(warehouseId, districtId, orderId));
    orderLines.forEach(
        line -> {
          int supplyWarehouseId = line.getValue(OrderLine.KEY_SUPPLY_W_ID).get().getAsInt();
          int itemId = line.getValue(OrderLine.KEY_ITEM_ID).get().getAsInt();
          int quantity = line.getValue(OrderLine.KEY_QUANTITY).get().getAsInt();
          double amount = line.getValue(OrderLine.KEY_AMOUNT).get().getAsDouble();
          Date deliveryDate = new Date(line.getValue(OrderLine.KEY_DELIVERY_D).get().getAsLong());
        });
  }

  @Override
  public void commit() throws TransactionException {
    transaction.commit();
  }

  @Override
  public void abort() throws TransactionException {
    if (transaction != null) {
      transaction.abort();
    }
  }
}
