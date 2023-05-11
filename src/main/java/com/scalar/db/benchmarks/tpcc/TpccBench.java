package com.scalar.db.benchmarks.tpcc;

import static com.scalar.db.benchmarks.Common.getDatabaseConfig;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.table.TpccRecord;
import com.scalar.db.benchmarks.tpcc.transaction.DeliveryTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.NewOrderTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.OrderStatusTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.PaymentTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.StockLevelTransaction;
import com.scalar.db.benchmarks.tpcc.transaction.TpccTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.TransactionFactory;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.json.Json;

public class TpccBench extends TimeBasedProcessor {
  private static final String CONFIG_NAME = "tpcc_config";
  private static final String NUM_WAREHOUSES = "num_warehouses";
  private static final String BACKOFF = "backoff";
  private static final String USE_TABLE_INDEX = "use_table_index";
  private static final String NP_ONLY = "np_only";
  private static final String RATE_NEW_ORDER = "rate_new_order";
  private static final String RATE_PAYMENT = "rate_payment";
  private static final String RATE_ORDER_STATUS = "rate_order_status";
  private static final String RATE_DELIVERY = "rate_delivery";
  private static final String RATE_STOCK_LEVEL = "rate_stock_level";
  private static final long DEFAULT_NUM_WAREHOUSES = 1;
  private static final long DEFAULT_BACKOFF = 0;
  private static final boolean DEFAULT_USE_TABLE_INDEX = false;
  private final DistributedTransactionManager manager;
  private final AtomicInteger abortCounter = new AtomicInteger();
  private final TpccConfig tpccConfig;
  private final int numWarehouses;
  private final int backoff;
  private final boolean useTableIndex;

  public TpccBench(Config config) {
    super(config);
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    TransactionFactory factory = new TransactionFactory(dbConfig);
    manager = factory.getTransactionManager();
    manager.withNamespace(TpccRecord.NAMESPACE);

    this.numWarehouses =
        (int) config.getUserLong(CONFIG_NAME, NUM_WAREHOUSES, DEFAULT_NUM_WAREHOUSES);
    this.backoff = (int) config.getUserLong(CONFIG_NAME, BACKOFF, DEFAULT_BACKOFF);
    this.useTableIndex =
        config.getUserBoolean(CONFIG_NAME, USE_TABLE_INDEX, DEFAULT_USE_TABLE_INDEX);
    if (config.hasUserValue(CONFIG_NAME, NP_ONLY) && config.getUserBoolean(CONFIG_NAME, NP_ONLY)) {
      if (hasRateParameter()) {
        throw new RuntimeException(
            NP_ONLY + " and rate parameters cannot be specified simultaneously");
      }
      tpccConfig =
          TpccConfig.newBuilder()
              .numWarehouse(numWarehouses)
              .npOnly()
              .backoff(backoff)
              .useTableIndex(useTableIndex)
              .build();
    } else if (hasRateParameter()) {
      if (!hasAllRateParameters()) {
        throw new RuntimeException(
            "specify all rate parameters to run the benchmark with your own mix");
      }
      tpccConfig =
          TpccConfig.newBuilder()
              .numWarehouse(numWarehouses)
              .rateNewOrder((int) config.getUserLong(CONFIG_NAME, RATE_NEW_ORDER))
              .ratePayment((int) config.getUserLong(CONFIG_NAME, RATE_PAYMENT))
              .rateOrderStatus((int) config.getUserLong(CONFIG_NAME, RATE_ORDER_STATUS))
              .rateDelivery((int) config.getUserLong(CONFIG_NAME, RATE_DELIVERY))
              .rateStockLevel((int) config.getUserLong(CONFIG_NAME, RATE_STOCK_LEVEL))
              .useTableIndex(useTableIndex)
              .backoff(backoff)
              .build();
    } else {
      tpccConfig =
          TpccConfig.newBuilder()
              .numWarehouse(numWarehouses)
              .fullMix()
              .backoff(backoff)
              .useTableIndex(useTableIndex)
              .build();
    }
  }

  @Override
  public void executeEach() throws TransactionException {
    TpccTransaction transaction = generateTpccTransaction();
    while (true) {
      try {
        transaction.execute();
        transaction.commit();
        break;
      } catch (CrudConflictException | CommitConflictException e) {
        transaction.abort();
        abortCounter.incrementAndGet();
        Uninterruptibles.sleepUninterruptibly(tpccConfig.getBackoff(), TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        transaction.abort();
        throw e;
      }
    }
  }

  @Override
  public void close() {
    setState(Json.createObjectBuilder().add("abort_count", abortCounter.toString()).build());
    manager.close();
  }

  private TpccTransaction generateTpccTransaction() throws TransactionException {
    int x = TpccUtil.randomInt(1, 100);
    if (x <= tpccConfig.getRateNewOrder()) {
      return new NewOrderTransaction(manager, tpccConfig);
    } else if (x <= tpccConfig.getRateNewOrder() + tpccConfig.getRatePayment()) {
      return new PaymentTransaction(manager, tpccConfig);
    } else if (x
        <= tpccConfig.getRateNewOrder()
            + tpccConfig.getRatePayment()
            + tpccConfig.getRateOrderStatus()) {
      return new OrderStatusTransaction(manager, tpccConfig);
    } else if (x
        <= tpccConfig.getRateNewOrder()
            + tpccConfig.getRatePayment()
            + tpccConfig.getRateOrderStatus()
            + tpccConfig.getRateDelivery()) {
      return new DeliveryTransaction(manager, tpccConfig);
    } else {
      return new StockLevelTransaction(manager, tpccConfig);
    }
  }

  private boolean hasRateParameter() {
    return config.hasUserValue(CONFIG_NAME, RATE_NEW_ORDER)
        || config.hasUserValue(CONFIG_NAME, RATE_PAYMENT)
        || config.hasUserValue(CONFIG_NAME, RATE_ORDER_STATUS)
        || config.hasUserValue(CONFIG_NAME, RATE_DELIVERY)
        || config.hasUserValue(CONFIG_NAME, RATE_STOCK_LEVEL);
  }

  private boolean hasAllRateParameters() {
    return config.hasUserValue(CONFIG_NAME, RATE_NEW_ORDER)
        && config.hasUserValue(CONFIG_NAME, RATE_PAYMENT)
        && config.hasUserValue(CONFIG_NAME, RATE_ORDER_STATUS)
        && config.hasUserValue(CONFIG_NAME, RATE_DELIVERY)
        && config.hasUserValue(CONFIG_NAME, RATE_STOCK_LEVEL);
  }
}
