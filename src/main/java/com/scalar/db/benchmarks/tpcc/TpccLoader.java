package com.scalar.db.benchmarks.tpcc;

import static com.scalar.db.benchmarks.Common.getDatabaseConfig;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.table.Customer;
import com.scalar.db.benchmarks.tpcc.table.CustomerSecondary;
import com.scalar.db.benchmarks.tpcc.table.District;
import com.scalar.db.benchmarks.tpcc.table.History;
import com.scalar.db.benchmarks.tpcc.table.Item;
import com.scalar.db.benchmarks.tpcc.table.NewOrder;
import com.scalar.db.benchmarks.tpcc.table.Order;
import com.scalar.db.benchmarks.tpcc.table.OrderLine;
import com.scalar.db.benchmarks.tpcc.table.OrderSecondary;
import com.scalar.db.benchmarks.tpcc.table.Stock;
import com.scalar.db.benchmarks.tpcc.table.TpccRecord;
import com.scalar.db.benchmarks.tpcc.table.Warehouse;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.TransactionFactory;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;

public class TpccLoader extends PreProcessor {
  private static final String CONFIG_NAME = "tpcc_config";
  private static final String LOAD_CONCURRENCY = "load_concurrency";
  private static final String NUM_WAREHOUSES = "num_warehouses";
  private static final String START_WAREHOUSE = "load_start_warehouse";
  private static final String END_WAREHOUSE = "load_end_warehouse";
  private static final String SKIP_ITEM_LOAD = "skip_item_load";
  private static final String USE_TABLE_INDEX = "use_table_index";
  private static final String CSV_FILE_DIRECTORY = "csv_file_directory";
  private static final long DEFAULT_LOAD_CONCURRENCY = 1;
  private static final long DEFAULT_START_WAREHOUSE = 1;
  private static final boolean DEFAULT_SKIP_ITEM_LOAD = false;
  private static final boolean DEFAULT_USE_TABLE_INDEX = false;
  private static final int QUEUE_SIZE = 10000;
  private static final String CUSTOMER = "customer.csv";
  private static final String CUSTOMER_SECONDARY = "customer_secondary.csv";
  private static final String DISTRICT = "district.csv";
  private static final String HISTORY = "history.csv";
  private static final String ITEM = "item.csv";
  private static final String NEW_ORDER = "new_order.csv";
  private static final String ORDER = "oorder.csv";
  private static final String ORDER_LINE = "order_line.csv";
  private static final String ORDER_SECONDARY = "order_secondary.csv";
  private static final String STOCK = "stock.csv";
  private static final String WAREHOUSE = "warehouse.csv";
  private static final String[] CUSTOMER_HEADER =
      "c_w_id,c_d_id,c_id,c_discount,c_credit,c_last,c_first,c_credit_lim,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_middle,c_data"
          .split(",");
  private static final String[] CUSTOMER_SECONDARY_HEADER =
      "c_w_id,c_d_id,c_last,c_first,c_id".split(",");
  private static final String[] DISTRICT_HEADER =
      "d_w_id,d_id,d_ytd,d_tax,d_next_o_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip"
          .split(",");
  private static final String[] HISTORY_HEADER =
      "h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data".split(",");
  private static final String[] ITEM_HEADER = "i_id,i_name,i_price,i_data,i_im_id".split(",");
  private static final String[] NEW_ORDER_HEADER = "no_w_id,no_d_id,no_o_id".split(",");
  private static final String[] ORDER_HEADER =
      "o_w_id,o_d_id,o_id,o_c_id,o_carrier_id,o_ol_cnt,o_all_local,o_entry_d".split(",");
  private static final String[] ORDER_LINE_HEADER =
      "ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info"
          .split(",");
  private static final String[] ORDER_SECONDARY_HEADER = "o_w_id,o_d_id,o_c_id,o_id".split(",");
  private static final String[] STOCK_HEADER =
      "s_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_data,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10"
          .split(",");
  private static final String[] WAREHOUSE_HEADER =
      "w_id,w_ytd,w_tax,w_name,w_street_1,w_street_2,w_city,w_state,w_zip".split(",");
  private static final Map<String, String[]> HEADER_MAP =
      ImmutableMap.<String, String[]>builder()
          .put(CUSTOMER, CUSTOMER_HEADER)
          .put(CUSTOMER_SECONDARY, CUSTOMER_SECONDARY_HEADER)
          .put(DISTRICT, DISTRICT_HEADER)
          .put(HISTORY, HISTORY_HEADER)
          .put(ITEM, ITEM_HEADER)
          .put(NEW_ORDER, NEW_ORDER_HEADER)
          .put(ORDER, ORDER_HEADER)
          .put(ORDER_LINE, ORDER_LINE_HEADER)
          .put(ORDER_SECONDARY, ORDER_SECONDARY_HEADER)
          .put(STOCK, STOCK_HEADER)
          .put(WAREHOUSE, WAREHOUSE_HEADER)
          .build();
  private final DistributedTransactionManager manager;
  private final int concurrency;
  private final int startWarehouse;
  private final int endWarehouse;
  private final boolean skipItemLoad;
  private final boolean useTableIndex;
  @Nullable private String directory;

  public TpccLoader(Config config) {
    super(config);
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    TransactionFactory factory = new TransactionFactory(dbConfig);
    manager = factory.getTransactionManager();
    manager.withNamespace(TpccRecord.NAMESPACE);

    this.concurrency =
        (int) config.getUserLong(CONFIG_NAME, LOAD_CONCURRENCY, DEFAULT_LOAD_CONCURRENCY);
    this.skipItemLoad = config.getUserBoolean(CONFIG_NAME, SKIP_ITEM_LOAD, DEFAULT_SKIP_ITEM_LOAD);
    this.useTableIndex =
        config.getUserBoolean(CONFIG_NAME, USE_TABLE_INDEX, DEFAULT_USE_TABLE_INDEX);
    if (config.hasUserValue(CONFIG_NAME, CSV_FILE_DIRECTORY)) {
      this.directory = config.getUserString(CONFIG_NAME, CSV_FILE_DIRECTORY);
    } else {
      this.directory = null;
    }

    if (config.hasUserValue(CONFIG_NAME, END_WAREHOUSE)
        && config.hasUserValue(CONFIG_NAME, NUM_WAREHOUSES)) {
      throw new RuntimeException(
          END_WAREHOUSE + " and " + NUM_WAREHOUSES + " cannot be specified simultaneously");
    }

    this.startWarehouse =
        (int) config.getUserLong(CONFIG_NAME, START_WAREHOUSE, DEFAULT_START_WAREHOUSE);
    if (!config.hasUserValue(CONFIG_NAME, END_WAREHOUSE)
        && !config.hasUserValue(CONFIG_NAME, NUM_WAREHOUSES)) {
      this.endWarehouse = this.startWarehouse;
    } else if (config.hasUserValue(CONFIG_NAME, NUM_WAREHOUSES)) {
      this.endWarehouse =
          this.startWarehouse + (int) config.getUserLong(CONFIG_NAME, NUM_WAREHOUSES) - 1;
    } else {
      this.endWarehouse = (int) config.getUserLong(CONFIG_NAME, END_WAREHOUSE);
    }
  }

  @Override
  public void execute() {
    ExecutorService executor = Executors.newFixedThreadPool(concurrency + 1);
    BlockingQueue<TpccRecord> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    AtomicBoolean isAllQueued = new AtomicBoolean();
    AtomicInteger queuedCounter = new AtomicInteger();
    AtomicInteger succeededCounter = new AtomicInteger();
    AtomicInteger failedCounter = new AtomicInteger();

    for (int i = 0; i < concurrency; ++i) {
      executor.execute(
          () -> {
            while (true) {
              TpccRecord record = queue.poll();
              if (record == null) {
                if (isAllQueued.get()) {
                  break;
                }
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                continue;
              }
              try {
                insert(manager, record);
                succeededCounter.incrementAndGet();
              } catch (Exception e) {
                e.printStackTrace();
                failedCounter.incrementAndGet();
              }
            }
          });
    }

    Future<?> future =
        executor.submit(
            () -> {
              while (!isAllQueued.get()
                  || succeededCounter.get() + failedCounter.get() < queuedCounter.get()) {
                logInfo(succeededCounter.get() + " succeeded, " + failedCounter + " failed");
                Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
              }
            });

    if (directory != null) {
      queueCsv(new File(directory, WAREHOUSE), queue, queuedCounter);
      queueCsv(new File(directory, ITEM), queue, queuedCounter);
      queueCsv(new File(directory, STOCK), queue, queuedCounter);
      queueCsv(new File(directory, DISTRICT), queue, queuedCounter);
      queueCsv(new File(directory, CUSTOMER), queue, queuedCounter);
      queueCsv(new File(directory, CUSTOMER_SECONDARY), queue, queuedCounter);
      queueCsv(new File(directory, HISTORY), queue, queuedCounter);
      queueCsv(new File(directory, ORDER), queue, queuedCounter);
      queueCsv(new File(directory, NEW_ORDER), queue, queuedCounter);
      queueCsv(new File(directory, ORDER_LINE), queue, queuedCounter);
      queueCsv(new File(directory, ORDER_SECONDARY), queue, queuedCounter);
    } else {
      try {
        if (!skipItemLoad) {
          for (int itemId = 1; itemId <= Item.ITEMS; itemId++) {
            queue.put(new Item(itemId));
            queuedCounter.incrementAndGet();
          }
        }
        queueWarehouses(queue, queuedCounter);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    isAllQueued.set(true);

    try {
      future.get();
      executor.shutdown();
      Uninterruptibles.awaitTerminationUninterruptibly(executor);
    } catch (java.util.concurrent.ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    logInfo("all records have been inserted");
  }

  @Override
  public void close() {
    manager.close();
  }

  private void queueWarehouses(BlockingQueue<TpccRecord> queue, AtomicInteger counter)
      throws InterruptedException {
    Date date = new Date();
    for (int warehouseId = startWarehouse; warehouseId <= endWarehouse; warehouseId++) {
      queue.put(new Warehouse(warehouseId));
      counter.incrementAndGet();
      for (int stockId = 1; stockId <= Warehouse.STOCKS; stockId++) {
        queue.put(new Stock(warehouseId, stockId));
        counter.incrementAndGet();
      }
      queueDistricts(queue, counter, warehouseId, date);
    }
  }

  private void queueDistricts(
      BlockingQueue<TpccRecord> queue, AtomicInteger counter, int warehouseId, Date date)
      throws InterruptedException {
    for (int districtId = 1; districtId <= Warehouse.DISTRICTS; districtId++) {
      queue.put(new District(warehouseId, districtId));
      counter.incrementAndGet();
      queueCustomers(queue, counter, warehouseId, districtId, date);
      queueOrders(queue, counter, warehouseId, districtId, date);
    }
  }

  private void queueCustomers(
      BlockingQueue<TpccRecord> queue,
      AtomicInteger counter,
      int warehouseId,
      int districtId,
      Date date)
      throws InterruptedException {
    for (int customerId = 1; customerId <= District.CUSTOMERS; customerId++) {
      Customer customer = new Customer(warehouseId, districtId, customerId, date);
      String last = customer.getLastName();
      String first = customer.getFirstName();
      // customer_secondary
      if (useTableIndex) {
        queue.put(new CustomerSecondary(warehouseId, districtId, last, first, customerId));
        counter.incrementAndGet();
      } else {
        customer.buildIndexColumn();
      }
      // customer
      queue.put(customer);
      counter.incrementAndGet();
      // history
      queue.put(new History(customerId, districtId, warehouseId, districtId, warehouseId, date));
      counter.incrementAndGet();
    }
  }

  private void queueOrders(
      BlockingQueue<TpccRecord> queue,
      AtomicInteger counter,
      int warehouseId,
      int districtId,
      Date date)
      throws InterruptedException {
    List<Integer> customers = new ArrayList<>();
    for (int customerId = 1; customerId <= District.CUSTOMERS; customerId++) {
      customers.add(customerId);
    }
    Collections.shuffle(customers);
    Integer[] permutation = customers.toArray(new Integer[District.CUSTOMERS]);

    for (int orderId = 1; orderId <= District.ORDERS; orderId++) {
      int customerId = permutation[orderId - 1];
      Order order = new Order(warehouseId, districtId, orderId, customerId, date);
      // order & order-secondary
      if (useTableIndex) {
        queue.put(new OrderSecondary(warehouseId, districtId, customerId, orderId));
        counter.incrementAndGet();
      } else {
        order.buildIndexColumn();
      }
      queue.put(order);
      counter.incrementAndGet();
      int orderLineCount = order.getOrderLineCount();
      for (int number = 1; number <= orderLineCount; number++) {
        int itemId = TpccUtil.randomInt(1, Item.ITEMS);
        // order-line
        queue.put(
            new OrderLine(warehouseId, districtId, orderId, number, warehouseId, itemId, date));
        counter.incrementAndGet();
      }
      if (orderId > 2100) {
        // new-order
        queue.put(new NewOrder(warehouseId, districtId, orderId));
        counter.incrementAndGet();
      }
    }
  }

  private void insert(DistributedTransactionManager manager, TpccRecord record)
      throws TransactionException {
    DistributedTransaction tx = manager.start();
    tx.withNamespace(TpccRecord.NAMESPACE);
    try {
      tx.put(record.createPut());
      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  private void queueCsv(File file, BlockingQueue<TpccRecord> queue, AtomicInteger counter) {
    CSVFormat format =
        CSVFormat.Builder.create(CSVFormat.DEFAULT)
            .setHeader(HEADER_MAP.get(file.getName()))
            .build();

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(new BOMInputStream(new FileInputStream(file))))) {
      CSVParser parser = CSVParser.parse(reader, format);
      for (CSVRecord record : parser) {
        switch (file.getName()) {
          case CUSTOMER:
            Customer customer = new Customer(record);
            if (!useTableIndex) {
              customer.buildIndexColumn();
            }
            queue.put(customer);
            break;
          case CUSTOMER_SECONDARY:
            queue.put(new CustomerSecondary(record));
            break;
          case DISTRICT:
            queue.put(new District(record));
            break;
          case HISTORY:
            queue.put(new History(record));
            break;
          case ITEM:
            queue.put(new Item(record));
            break;
          case NEW_ORDER:
            queue.put(new NewOrder(record));
            break;
          case ORDER:
            queue.put(new Order(record));
            break;
          case ORDER_LINE:
            queue.put(new OrderLine(record));
            break;
          case ORDER_SECONDARY:
            queue.put(new OrderSecondary(record));
            break;
          case STOCK:
            queue.put(new Stock(record));
            break;
          case WAREHOUSE:
            queue.put(new Warehouse(record));
            break;
          default:
        }
        counter.incrementAndGet();
      }
    } catch (Exception e) {
      throw new RuntimeException("failed to load a CSV file: " + file.getPath(), e);
    }
  }
}
