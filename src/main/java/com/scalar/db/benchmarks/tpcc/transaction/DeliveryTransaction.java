package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.benchmarks.tpcc.TpccConfig;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.benchmarks.tpcc.table.Customer;
import com.scalar.db.benchmarks.tpcc.table.NewOrder;
import com.scalar.db.benchmarks.tpcc.table.Order;
import com.scalar.db.benchmarks.tpcc.table.OrderLine;
import com.scalar.db.benchmarks.tpcc.table.Warehouse;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class DeliveryTransaction implements TpccTransaction {
  private final TpccConfig config;
  private final DistributedTransactionManager manager;
  private DistributedTransaction transaction;
  private int warehouseId;
  private int carrierId;
  private Date deliveryDate;

  public DeliveryTransaction(DistributedTransactionManager manager, TpccConfig config) {
    this.manager = manager;
    this.config = config;
    generate();
  }

  private void generate() {
    int numWarehouse = config.getNumWarehouse();
    warehouseId = TpccUtil.randomInt(1, numWarehouse);
    carrierId = TpccUtil.randomInt(1, 10);
    deliveryDate = new Date();
  }

  @Override
  public void execute() throws TransactionException {
    transaction = manager.start();

    for (int districtId = 1; districtId <= Warehouse.DISTRICTS; districtId++) {
      // Get the oldest outstanding new-order
      List<Result> newOrders = transaction.scan(NewOrder.createScan(warehouseId, districtId));
      if (newOrders.size() != 1) {
        throw new TransactionException("Invalid scan on new-order");
      }
      int orderId = newOrders.get(0).getValue(NewOrder.KEY_ORDER_ID).get().getAsInt();

      // Delete the new-order
      transaction.delete(NewOrder.createDelete(warehouseId, districtId, orderId));

      // Get the customer of the new-order
      Optional<Result> result = transaction.get(Order.createGet(warehouseId, districtId, orderId));
      if (!result.isPresent()) {
        throw new TransactionException("Order not found");
      }
      int customerId = result.get().getValue(Order.KEY_CUSTOMER_ID).get().getAsInt();

      // Update the carrier ID
      Order order = new Order(warehouseId, districtId, orderId, carrierId);
      transaction.put(order.createPut());

      // Get and update order-lines
      double total = 0;
      List<Result> orderLines =
          transaction.scan(OrderLine.createScan(warehouseId, districtId, orderId));
      for (Result line : orderLines) {
        int number = line.getValue(OrderLine.KEY_NUMBER).get().getAsInt();
        total += line.getValue(OrderLine.KEY_AMOUNT).get().getAsDouble();
        OrderLine newLine = new OrderLine(warehouseId, districtId, orderId, number, deliveryDate);
        transaction.put(newLine.createPut());
      }

      // Update the customer with new balance and delivery count
      result = transaction.get(Customer.createGet(warehouseId, districtId, customerId));
      if (!result.isPresent()) {
        throw new TransactionException("Customer not found");
      }
      double balance = result.get().getValue(Customer.KEY_BALANCE).get().getAsDouble() + total;
      int deliveryCount = result.get().getValue(Customer.KEY_DELIVERY_CNT).get().getAsInt() + 1;
      Customer customer = new Customer(warehouseId, districtId, customerId, balance, deliveryCount);
      transaction.put(customer.createPut());
    }
  }

  @Override
  public void commit() throws TransactionException {
    transaction.commit();
  }

  @Override
  public void abort() throws TransactionException {
    transaction.abort();
  }
}
