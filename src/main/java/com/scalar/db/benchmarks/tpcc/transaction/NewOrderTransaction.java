package com.scalar.db.benchmarks.tpcc.transaction;

import java.util.Date;
import java.util.Optional;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Customer;
import com.scalar.db.benchmarks.tpcc.TPCCTable.District;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Item;
import com.scalar.db.benchmarks.tpcc.TPCCTable.NewOrder;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Order;
import com.scalar.db.benchmarks.tpcc.TPCCTable.OrderLine;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Stock;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Warehouse;
import com.scalar.db.benchmarks.tpcc.TPCCUtil;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;

public class NewOrderTransaction implements TPCCTransaction {
  private int warehouseId;
  private int districtId;
  private int customerId;
  private int rollback;
  private int orderLineCount;
  private int[] itemIds;
  private int[] supplierWarehouseIds;
  private int[] orderQuantities;
  private boolean remote;
  private Date date;

  public void generate(int numWarehouse) {
    warehouseId = TPCCUtil.randomInt(1, numWarehouse);
    districtId = TPCCUtil.randomInt(1, Warehouse.DISTRICTS);
    customerId = TPCCUtil.getCustomerId();
    rollback = TPCCUtil.randomInt(1, 100);
    orderLineCount = TPCCUtil.randomInt(5, 15);
    itemIds = new int[orderLineCount];
    supplierWarehouseIds = new int[orderLineCount];
    orderQuantities = new int[orderLineCount];
    remote = false;
    date = new Date();

    for (int i = 0; i < orderLineCount; i++) {
      itemIds[i] = TPCCUtil.getItemId();
      if (numWarehouse == 1 || TPCCUtil.randomInt(1, 100) > 1) {
        supplierWarehouseIds[i] = warehouseId;
      } else {
        do {
          supplierWarehouseIds[i] = TPCCUtil.randomInt(1, numWarehouse);
        } while (supplierWarehouseIds[i] == warehouseId);
        remote = true;
      }
      orderQuantities[i] = TPCCUtil.randomInt(1, 10);
    }

    if (rollback == 1) {
      // set an unused item number to produce "not-found" for roll back
      itemIds[orderLineCount - 1] += Item.ITEMS;
    }
  }

  private String getDistInfo(Optional<Result> stock, int districtId) throws TransactionException {
    switch (districtId) {
      case 1:
        return stock.get().getValue(Stock.KEY_DISTRICT01).get().getAsString().get();
      case 2:
        return stock.get().getValue(Stock.KEY_DISTRICT02).get().getAsString().get();
      case 3:
        return stock.get().getValue(Stock.KEY_DISTRICT03).get().getAsString().get();
      case 4:
        return stock.get().getValue(Stock.KEY_DISTRICT04).get().getAsString().get();
      case 5:
        return stock.get().getValue(Stock.KEY_DISTRICT05).get().getAsString().get();
      case 6:
        return stock.get().getValue(Stock.KEY_DISTRICT06).get().getAsString().get();
      case 7:
        return stock.get().getValue(Stock.KEY_DISTRICT07).get().getAsString().get();
      case 8:
        return stock.get().getValue(Stock.KEY_DISTRICT08).get().getAsString().get();
      case 9:
        return stock.get().getValue(Stock.KEY_DISTRICT09).get().getAsString().get();
      case 10:
        return stock.get().getValue(Stock.KEY_DISTRICT10).get().getAsString().get();
      default:
        throw new TransactionException("No such district ID");
    }
  }

  public void execute(DistributedTransactionManager manager) throws TransactionException {
    DistributedTransaction tx = manager.start();

    try {
      Get get;
      Put put;
      Optional<Result> result;

      // Get warehouse
      get = new Get(Warehouse.createPartitionKey(warehouseId));
      result = tx.get(get.forTable(Warehouse.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("Warehouse not found");
      }
      double warehouseTax = result.get().getValue(Warehouse.KEY_TAX).get().getAsDouble();

      // Get and update district
      Key districtKey = District.createPartitionKey(warehouseId, districtId);
      get = new Get(districtKey);
      result = tx.get(get.forTable(District.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("District not found");
      }
      double districtTax = result.get().getValue(District.KEY_TAX).get().getAsDouble();
      int orderId = result.get().getValue(District.KEY_NEXT_O_ID).get().getAsInt();
      put = new Put(districtKey).withValue(District.KEY_NEXT_O_ID, orderId + 1);
      tx.put(put.forTable(District.TABLE_NAME));

      // Get customer
      get = new Get(Customer.createPartitionKey(warehouseId, districtId, customerId));
      result = tx.get(get.forTable(Customer.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("Customer not found");
      }
      double discount = result.get().getValue(Customer.KEY_DISCOUNT).get().getAsDouble();

      // Insert new-order
      NewOrder newOrder = new NewOrder(warehouseId, districtId, orderId);
      put = new Put(newOrder.createPartitionKey(), newOrder.createClusteringKey());
      tx.put(put.forTable(NewOrder.TABLE_NAME));

      // Insert order
      Order order = new Order(warehouseId, districtId, orderId, customerId, 0, orderLineCount,
          remote ? 0 : 1, date);
      put = new Put(order.createPartitionKey(), order.createClusteringKey())
          .withValues(order.createValues());
      tx.put(put.forTable(Order.TABLE_NAME));

      // Insert order-line
      for (int orderLineNumber = 1; orderLineNumber <= orderLineCount; orderLineNumber++) {
        int itemId = itemIds[orderLineNumber - 1];
        int supplyWarehouseId = supplierWarehouseIds[orderLineNumber - 1];
        int quantity = orderQuantities[orderLineNumber - 1];

        // Get item
        get = new Get(Item.createPartitionKey(itemId));
        result = tx.get(get.forTable(Item.TABLE_NAME));
        if (!result.isPresent()) {
          throw new TransactionException("Item not found");
        }
        double itemPrice = result.get().getValue(Item.KEY_PRICE).get().getAsDouble();
        double amount =
            quantity * itemPrice * (1.0 + warehouseTax + districtTax) * (1.0 - discount);

        // Get and update stock
        Key stockKey = Stock.createPartitionKey(warehouseId, itemId);
        get = new Get(stockKey);
        result = tx.get(get.forTable(Stock.TABLE_NAME));
        if (!result.isPresent()) {
          throw new TransactionException("Stock not found");
        }
        double stockYTD = result.get().getValue(Stock.KEY_YTD).get().getAsDouble() + quantity;
        int stockOrderCount = result.get().getValue(Stock.KEY_ORDER_CNT).get().getAsInt() + 1;
        int stockRemoteCount = result.get().getValue(Stock.KEY_REMOTE_CNT).get().getAsInt();
        if (remote) {
          stockRemoteCount++;
        }
        int stockQuantity = result.get().getValue(Stock.KEY_QUANTITY).get().getAsInt();
        if (stockQuantity > quantity + 10) {
          stockQuantity -= quantity;
        } else {
          stockQuantity = (stockQuantity - quantity) + 91;
        }
        String distInfo = getDistInfo(result, districtId);
        Stock stock =
            new Stock(warehouseId, itemId, quantity, stockYTD, stockOrderCount, stockRemoteCount);
        tx.put(new Put(stock.createPartitionKey()).withValues(stock.createValues())
            .forTable(Stock.TABLE_NAME));

        // Insert order-line
        OrderLine orderLine = new OrderLine(warehouseId, districtId, orderId, orderLineNumber,
            supplyWarehouseId, amount, quantity, itemId, distInfo);
        tx.put(new Put(orderLine.createPartitionKey(), orderLine.createClusteringKey())
            .withValues(orderLine.createValues()).forTable(OrderLine.TABLE_NAME));
      }

      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }
}
