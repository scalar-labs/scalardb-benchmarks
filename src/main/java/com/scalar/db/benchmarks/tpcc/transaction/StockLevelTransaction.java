package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.benchmarks.tpcc.TpccConfig;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.benchmarks.tpcc.table.District;
import com.scalar.db.benchmarks.tpcc.table.Item;
import com.scalar.db.benchmarks.tpcc.table.OrderLine;
import com.scalar.db.benchmarks.tpcc.table.Stock;
import com.scalar.db.benchmarks.tpcc.table.Warehouse;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class StockLevelTransaction implements TpccTransaction {
  private static final int NUM_LATEST_ORDERS = 20;
  private final TpccConfig config;
  private final DistributedTransactionManager manager;
  private DistributedTransaction transaction;
  private int warehouseId;
  private int districtId;
  private int threshold;

  public StockLevelTransaction(DistributedTransactionManager manager, TpccConfig config) {
    this.manager = manager;
    this.config = config;
    generate();
  }

  private void generate() {
    int numWarehouse = config.getNumWarehouse();
    warehouseId = TpccUtil.randomInt(1, numWarehouse);
    districtId = TpccUtil.randomInt(1, Warehouse.DISTRICTS);
    threshold = TpccUtil.randomInt(10, 20);
  }

  @Override
  public void execute() throws TransactionException {
    transaction = manager.start();

    // Get next order ID in the district
    Optional<Result> result = transaction.get(District.createGet(warehouseId, districtId));
    if (!result.isPresent()) {
      throw new TransactionException("District not found");
    }
    int orderId = result.get().getValue(District.KEY_NEXT_O_ID).get().getAsInt();

    // Get order-lines of the last 20 orders
    List<Result> orderLines =
        transaction.scan(
            OrderLine.createScan(
                warehouseId, districtId, orderId - NUM_LATEST_ORDERS, orderId - 1));

    // Prepare distinct items
    Set<Integer> itemSet = new HashSet<>();
    for (Result line : orderLines) {
      int itemId = line.getValue(OrderLine.KEY_ITEM_ID).get().getAsInt();
      if (itemId != Item.UNUSED_ID) {
        itemSet.add(itemId);
      }
    }

    // Count items where its stock is below the threshold
    int lowStock = 0;
    for (int itemId : itemSet) {
      Optional<Result> stock = transaction.get(Stock.createGet(warehouseId, itemId));
      if (!stock.isPresent()) {
        throw new TransactionException("Stock not found");
      }
      int quantity = stock.get().getValue(Stock.KEY_QUANTITY).get().getAsInt();
      if (quantity < threshold) {
        lowStock++;
      }
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
