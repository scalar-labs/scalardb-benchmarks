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
  private int warehouseId;
  private int carrierId;
  private Date deliveryDate;

  public DeliveryTransaction(TpccConfig c) {
    config = c;
  }

  /** Generates arguments for the delivery transaction. */
  @Override
  public void generate() {
    int numWarehouse = config.getNumWarehouse();
    warehouseId = TpccUtil.randomInt(1, numWarehouse);
    carrierId = TpccUtil.randomInt(1, 10);
    deliveryDate = new Date();
  }

  /**
   * Executes the delivery transaction.
   *
   * @param manager a {@code DistributedTransactionManager} object
   */
  @Override
  public void execute(DistributedTransactionManager manager) throws TransactionException {
    DistributedTransaction tx = manager.start();

    try {
      for (int districtId = 1; districtId <= Warehouse.DISTRICTS; districtId++) {
        // Get the oldest outstanding new-order
        List<Result> newOrders = tx.scan(NewOrder.createScan(warehouseId, districtId));
        if (newOrders.size() != 1) {
          throw new TransactionException("Invalid scan on new-order");
        }
        int orderId = newOrders.get(0).getValue(NewOrder.KEY_ORDER_ID).get().getAsInt();

        // Delete the new-order
        tx.delete(NewOrder.createDelete(warehouseId, districtId, orderId));

        // Get the customer of the new-order
        Optional<Result> result = tx.get(Order.createGet(warehouseId, districtId, orderId));
        if (!result.isPresent()) {
          throw new TransactionException("Order not found");
        }
        int customerId = result.get().getValue(Order.KEY_CUSTOMER_ID).get().getAsInt();

        // Update the carrier ID
        Order order = new Order(warehouseId, districtId, orderId, carrierId);
        tx.put(order.createPut());

        // Get and update order-lines
        double total = 0;
        List<Result> orderLines = tx.scan(OrderLine.createScan(warehouseId, districtId, orderId));
        for (Result line : orderLines) {
          int number = line.getValue(OrderLine.KEY_NUMBER).get().getAsInt();
          total += line.getValue(OrderLine.KEY_AMOUNT).get().getAsDouble();
          OrderLine newLine = new OrderLine(warehouseId, districtId, orderId, number, deliveryDate);
          tx.put(newLine.createPut());
        }

        // Update the customer with new balance and delivery count
        result = tx.get(Customer.createGet(warehouseId, districtId, customerId));
        if (!result.isPresent()) {
          throw new TransactionException("Customer not found");
        }
        double balance = result.get().getValue(Customer.KEY_BALANCE).get().getAsDouble() + total;
        int deliveryCount = result.get().getValue(Customer.KEY_DELIVERY_CNT).get().getAsInt() + 1;
        Customer customer =
            new Customer(warehouseId, districtId, customerId, balance, deliveryCount);
        tx.put(customer.createPut());
      }

      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }
}
