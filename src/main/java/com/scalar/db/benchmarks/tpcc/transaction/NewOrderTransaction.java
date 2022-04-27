package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.benchmarks.tpcc.table.Customer;
import com.scalar.db.benchmarks.tpcc.table.District;
import com.scalar.db.benchmarks.tpcc.table.Item;
import com.scalar.db.benchmarks.tpcc.table.NewOrder;
import com.scalar.db.benchmarks.tpcc.table.Order;
import com.scalar.db.benchmarks.tpcc.table.OrderLine;
import com.scalar.db.benchmarks.tpcc.table.Stock;
import com.scalar.db.benchmarks.tpcc.table.Warehouse;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Date;
import java.util.Optional;

public class NewOrderTransaction implements TpccTransaction {
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

  /**
   * Generates arguments for the new-order transaction.
   * 
   * @param numWarehouse a number of warehouse
   */
  public void generate(int numWarehouse) {
    warehouseId = TpccUtil.randomInt(1, numWarehouse);
    districtId = TpccUtil.randomInt(1, Warehouse.DISTRICTS);
    customerId = TpccUtil.getCustomerId();
    rollback = TpccUtil.randomInt(1, 100);
    orderLineCount = TpccUtil.randomInt(5, 15);
    itemIds = new int[orderLineCount];
    supplierWarehouseIds = new int[orderLineCount];
    orderQuantities = new int[orderLineCount];
    remote = false;
    date = new Date();

    for (int i = 0; i < orderLineCount; i++) {
      itemIds[i] = TpccUtil.getItemId();
      if (numWarehouse == 1 || TpccUtil.randomInt(1, 100) > 1) {
        supplierWarehouseIds[i] = warehouseId;
      } else {
        do {
          supplierWarehouseIds[i] = TpccUtil.randomInt(1, numWarehouse);
        } while (supplierWarehouseIds[i] == warehouseId);
        remote = true;
      }
      orderQuantities[i] = TpccUtil.randomInt(1, 10);
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

  /**
   * Executes the new-order transaction.
   * 
   * @param manager a {@code DistributedTransactionManager} object
   */
  public void execute(DistributedTransactionManager manager) throws TransactionException {
    DistributedTransaction tx = manager.start();

    try {
      // Get warehouse
      Optional<Result> result = tx.get(Warehouse.createGet(warehouseId));
      if (!result.isPresent()) {
        throw new TransactionException("Warehouse not found");
      }
      final double warehouseTax = result.get().getValue(Warehouse.KEY_TAX).get().getAsDouble();

      // Get and update district
      result = tx.get(District.createGet(warehouseId, districtId));
      if (!result.isPresent()) {
        throw new TransactionException("District not found");
      }
      final double districtTax = result.get().getValue(District.KEY_TAX).get().getAsDouble();
      final int orderId = result.get().getValue(District.KEY_NEXT_O_ID).get().getAsInt();
      District district = new District(warehouseId, districtId, orderId + 1);
      tx.put(district.createPut());

      // Get customer
      result = tx.get(Customer.createGet(warehouseId, districtId, customerId));
      if (!result.isPresent()) {
        throw new TransactionException("Customer not found");
      }
      double discount = result.get().getValue(Customer.KEY_DISCOUNT).get().getAsDouble();

      // Insert new-order
      NewOrder newOrder = new NewOrder(warehouseId, districtId, orderId);
      tx.put(newOrder.createPut());

      // Insert order
      Order order = new Order(warehouseId, districtId, orderId, customerId, 0, orderLineCount,
          remote ? 0 : 1, date);
      tx.put(order.createPut());

      // Insert order-line
      for (int orderLineNumber = 1; orderLineNumber <= orderLineCount; orderLineNumber++) {
        final int itemId = itemIds[orderLineNumber - 1];
        final int supplyWarehouseId = supplierWarehouseIds[orderLineNumber - 1];
        final int quantity = orderQuantities[orderLineNumber - 1];

        // Get item
        result = tx.get(Item.createGet(itemId));
        if (!result.isPresent()) {
          throw new TransactionException("Item not found");
        }
        final double itemPrice = result.get().getValue(Item.KEY_PRICE).get().getAsDouble();
        final double amount =
            quantity * itemPrice * (1.0 + warehouseTax + districtTax) * (1.0 - discount);

        // Get and update stock
        result = tx.get(Stock.createGet(warehouseId, itemId));
        if (!result.isPresent()) {
          throw new TransactionException("Stock not found");
        }
        double stockYtd = result.get().getValue(Stock.KEY_YTD).get().getAsDouble() + quantity;
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
        Stock stock = new Stock(warehouseId, itemId,
            stockQuantity, stockYtd, stockOrderCount, stockRemoteCount);
        tx.put(stock.createPut());

        // Insert order-line
        OrderLine orderLine = new OrderLine(warehouseId, districtId, orderId, orderLineNumber,
            supplyWarehouseId, amount, quantity, itemId, distInfo);
        tx.put(orderLine.createPut());
      }

      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }
}
