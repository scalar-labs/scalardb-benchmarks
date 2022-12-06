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

  private final TpccConfig config;
  private int warehouseId;
  private int districtId;
  private int threshold;

  public StockLevelTransaction(TpccConfig c) {
    config = c;
  }

  /** Generates arguments for the stock-level transaction. */
  @Override
  public void generate() {
    int numWarehouse = config.getNumWarehouse();
    warehouseId = TpccUtil.randomInt(1, numWarehouse);
    districtId = TpccUtil.randomInt(1, Warehouse.DISTRICTS);
    threshold = TpccUtil.randomInt(10, 20);
  }

  /**
   * Executes the stock-level transaction.
   *
   * @param manager a {@code DistributedTransactionManager} object
   */
  @Override
  public void execute(DistributedTransactionManager manager) throws TransactionException {
    DistributedTransaction tx = manager.start();

    try {
      // Get next order ID in the district
      Optional<Result> result = tx.get(District.createGet(warehouseId, districtId));
      if (!result.isPresent()) {
        throw new TransactionException("District not found");
      }
      int orderId = result.get().getValue(District.KEY_NEXT_O_ID).get().getAsInt();

      // Get order-lines of the last 20 orders
      List<Result> orderLines =
          tx.scan(OrderLine.createScan(warehouseId, districtId, orderId - 20, orderId - 1));

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
        Optional<Result> stock = tx.get(Stock.createGet(warehouseId, itemId));
        if (!stock.isPresent()) {
          throw new TransactionException("Stock not found");
        }
        int quantity = stock.get().getValue(Stock.KEY_QUANTITY).get().getAsInt();
        if (quantity < threshold) {
          lowStock++;
        }
      }

      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }
}
