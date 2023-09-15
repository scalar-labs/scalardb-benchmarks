package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.benchmarks.tpcc.TpccConfig;
import com.scalar.db.benchmarks.tpcc.TpccRollbackException;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.benchmarks.tpcc.table.Customer;
import com.scalar.db.benchmarks.tpcc.table.District;
import com.scalar.db.benchmarks.tpcc.table.Item;
import com.scalar.db.benchmarks.tpcc.table.NewOrder;
import com.scalar.db.benchmarks.tpcc.table.Order;
import com.scalar.db.benchmarks.tpcc.table.OrderLine;
import com.scalar.db.benchmarks.tpcc.table.OrderSecondary;
import com.scalar.db.benchmarks.tpcc.table.Stock;
import com.scalar.db.benchmarks.tpcc.table.Warehouse;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Date;
import java.util.Optional;

public class NewOrderTransaction implements TpccTransaction {
  private final TpccConfig config;
  private final DistributedTransactionManager manager;
  private DistributedTransaction transaction;
  private int warehouseId;
  private int districtId;
  private int customerId;
  private int orderLineCount;
  private int[] itemIds;
  private int[] supplierWarehouseIds;
  private int[] orderQuantities;
  private boolean remote;
  private Date date;

  public NewOrderTransaction(DistributedTransactionManager manager, TpccConfig config) {
    this.manager = manager;
    this.config = config;
    generate();
  }

  private void generate() {
    int numWarehouse = config.getNumWarehouse();
    warehouseId = TpccUtil.randomInt(1, numWarehouse);
    districtId = TpccUtil.randomInt(1, Warehouse.DISTRICTS);
    customerId = TpccUtil.getCustomerId();
    orderLineCount = TpccUtil.randomInt(5, 15);
    itemIds = new int[orderLineCount];
    supplierWarehouseIds = new int[orderLineCount];
    orderQuantities = new int[orderLineCount];
    remote = false;
    date = new Date();
    int rollback = TpccUtil.randomInt(1, 100);

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

  private String getDistInfo(Optional<Result> stock) {
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
        throw new IllegalStateException(
            String.format(
                "No such district ID: warehouse: %d, district: %d", warehouseId, districtId));
    }
  }

  @Override
  public void execute() throws TransactionException, TpccRollbackException {
    transaction = manager.start();

    // Get warehouse
    Optional<Result> result = transaction.get(Warehouse.createGet(warehouseId));
    if (!result.isPresent()) {
      throw new IllegalStateException(
          String.format("Warehouse not found: warehouse: %d", warehouseId));
    }
    final double warehouseTax = result.get().getValue(Warehouse.KEY_TAX).get().getAsDouble();

    // Get and update district
    result = transaction.get(District.createGet(warehouseId, districtId));
    if (!result.isPresent()) {
      throw new IllegalStateException(
          String.format(
              "District not found: warehouse: %d, district: %d", warehouseId, districtId));
    }
    final double districtTax = result.get().getValue(District.KEY_TAX).get().getAsDouble();
    final int orderId = result.get().getValue(District.KEY_NEXT_O_ID).get().getAsInt();
    District district = new District(warehouseId, districtId, orderId + 1);
    transaction.put(district.createPut());

    // Get customer
    result = transaction.get(Customer.createGet(warehouseId, districtId, customerId));
    if (!result.isPresent()) {
      throw new IllegalStateException(
          String.format(
              "Customer not found: warehouse: %d, district: %d, customer: %d",
              warehouseId, districtId, customerId));
    }
    double discount = result.get().getValue(Customer.KEY_DISCOUNT).get().getAsDouble();

    // Insert new-order
    NewOrder newOrder = new NewOrder(warehouseId, districtId, orderId);
    transaction.put(newOrder.createPut());

    // Insert order
    Order order =
        new Order(
            warehouseId, districtId, orderId, customerId, 0, orderLineCount, remote ? 0 : 1, date);
    if (!config.useTableIndex()) {
      order.buildIndexColumn();
    }
    transaction.put(order.createPut());

    // Insert order's secondary index
    if (!config.isNpOnly() && config.useTableIndex()) {
      OrderSecondary orderSecondary =
          new OrderSecondary(warehouseId, districtId, customerId, orderId);
      transaction.put(orderSecondary.createPut());
    }

    // Insert order-line
    for (int orderLineNumber = 1; orderLineNumber <= orderLineCount; orderLineNumber++) {
      final int itemId = itemIds[orderLineNumber - 1];
      final int supplyWarehouseId = supplierWarehouseIds[orderLineNumber - 1];
      final int quantity = orderQuantities[orderLineNumber - 1];

      // Get item
      result = transaction.get(Item.createGet(itemId));
      if (!result.isPresent()) {
        throw new TpccRollbackException(
            String.format(
                "Item not found: warehouse: %d, district: %d, item: %d",
                warehouseId, districtId, itemId));
      }
      final double itemPrice = result.get().getValue(Item.KEY_PRICE).get().getAsDouble();
      final double amount =
          quantity * itemPrice * (1.0 + warehouseTax + districtTax) * (1.0 - discount);

      // Get and update stock
      result = transaction.get(Stock.createGet(supplyWarehouseId, itemId));
      if (!result.isPresent()) {
        throw new IllegalStateException(
            String.format(
                "Stock not found: warehouse: %d, district: %d, supplier warehouse: %d, item: %d",
                warehouseId, districtId, supplyWarehouseId, itemId));
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
      String distInfo = getDistInfo(result);
      Stock stock =
          new Stock(
              supplyWarehouseId,
              itemId,
              stockQuantity,
              stockYtd,
              stockOrderCount,
              stockRemoteCount);
      transaction.put(stock.createPut());

      // Insert order-line
      OrderLine orderLine =
          new OrderLine(
              warehouseId,
              districtId,
              orderId,
              orderLineNumber,
              supplyWarehouseId,
              amount,
              quantity,
              itemId,
              distInfo);
      transaction.put(orderLine.createPut());
    }
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
