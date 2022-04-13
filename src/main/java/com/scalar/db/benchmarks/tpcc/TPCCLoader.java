package com.scalar.db.benchmarks.tpcc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Customer;
import com.scalar.db.benchmarks.tpcc.TPCCTable.CustomerSecondary;
import com.scalar.db.benchmarks.tpcc.TPCCTable.District;
import com.scalar.db.benchmarks.tpcc.TPCCTable.History;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Item;
import com.scalar.db.benchmarks.tpcc.TPCCTable.NewOrder;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Order;
import com.scalar.db.benchmarks.tpcc.TPCCTable.OrderLine;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Stock;
import com.scalar.db.benchmarks.tpcc.TPCCTable.TPCCRecord;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Warehouse;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.TransactionFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "tpcc-loader", description = "Load tables for TPC-C benchmark.")
public class TPCCLoader implements Callable<Integer> {

  @CommandLine.Option(
      names = {"--properties", "--config"},
      required = true,
      paramLabel = "PROPERTIES_FILE",
      description = "A configuration file in properties format.")
  private String properties;

  @CommandLine.Option(
      names = {"--num-warehouse"},
      required = false,
      paramLabel = "NUM_WAREHOUSE",
      description = "The number of warehouse.")
  private int numWarehouse = 1;

  @CommandLine.Option(
      names = {"--directory"},
      required = false,
      paramLabel = "CSV_DIRECTORY",
      description = "A directory that contains csv files.")
  private String directory = null;

  @CommandLine.Option(
      names = {"--num-threads"},
      required = false,
      paramLabel = "NUM_THREADS",
      description = "The number of threads to run.")
  private int numThreads = 1;

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "display the help message.")
  boolean helpRequested;

  private static final String customer = "customer.csv";
  private static final String customerSecondary = "customer_secondary.csv";
  private static final String district = "district.csv";
  private static final String history = "history.csv";
  private static final String item = "item.csv";
  private static final String newOrder = "new_order.csv";
  private static final String order = "oorder.csv";
  private static final String orderLine = "order_line.csv";
  private static final String stock = "stock.csv";
  private static final String warehouse = "warehouse.csv";
  private static final String[] customerHeaders = "c_w_id,c_d_id,c_id,c_discount,c_credit,c_last,c_first,c_credit_lim,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_middle,c_data"
      .split(",");
  private static final String[] customerSecondaryHeaders = "c_w_id,c_d_id,c_last,c_first,c_id"
      .split(",");
  private static final String[] districtHeaders = "d_w_id,d_id,d_ytd,d_tax,d_next_o_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip"
      .split(",");
  private static final String[] historyHeaders = "h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data"
      .split(",");
  private static final String[] itemHeaders = "i_id,i_name,i_price,i_data,i_im_id".split(",");
  private static final String[] newOrdersHeaders = "no_w_id,no_d_id,no_o_id".split(",");
  private static final String[] oOrdersHeaders = "o_w_id,o_d_id,o_id,o_c_id,o_carrier_id,o_ol_cnt,o_all_local,o_entry_d"
      .split(",");
  private static final String[] orderLineHeaders = "ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info"
      .split(",");
  private static final String[] stockHeaders = "s_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_data,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10"
      .split(",");
  private static final String[] warehouseHeaders = "w_id,w_ytd,w_tax,w_name,w_street_1,w_street_2,w_city,w_state,w_zip"
      .split(",");
  private static final Map<String, String[]> headerMap = ImmutableMap.<String, String[]>builder()
      .put(customer, customerHeaders)
      .put(customerSecondary, customerSecondaryHeaders)
      .put(district, districtHeaders)
      .put(history, historyHeaders)
      .put(item, itemHeaders)
      .put(newOrder, newOrdersHeaders)
      .put(order, oOrdersHeaders)
      .put(orderLine, orderLineHeaders)
      .put(stock, stockHeaders)
      .put(warehouse, warehouseHeaders)
      .build();

  public static void main(String[] args) {
    int exitCode = new CommandLine(new TPCCLoader()).execute(args);
    System.exit(exitCode);
  }

  public interface LoadWrapper<T, U> {
    void loader(T t, U u) throws TransactionException, ParseException;

    default boolean apply(T t, U u) {
      try {
        loader(t, u);
        return true;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }
  }

  @Override
  public Integer call() throws Exception {
    DatabaseConfig dbConfig = new DatabaseConfig(new FileInputStream(properties));
    TransactionFactory factory = new TransactionFactory(dbConfig);
    DistributedTransactionManager manager = factory.getTransactionManager();
    load(manager);
    return 0;
  }

  private void queueWarehouses(BlockingQueue<TPCCRecord> queue, AtomicInteger counter)
      throws InterruptedException {
    Date date = new Date();
    for (int warehouseId = 1; warehouseId <= numWarehouse; warehouseId++) {
      queue.put(new Warehouse(warehouseId));
      counter.incrementAndGet();
      for (int stockId = 1; stockId <= Warehouse.STOCKS; stockId++) {
        queue.put(new Stock(warehouseId, stockId));
        counter.incrementAndGet();
      }
      queueDistricts(queue, counter, warehouseId, date);
    }
  }

  private void queueDistricts(BlockingQueue<TPCCRecord> queue, AtomicInteger counter,
      int warehouseId, Date date) throws InterruptedException {
    for (int districtId = 1; districtId <= Warehouse.DISTRICTS; districtId++) {
      queue.put(new District(warehouseId, districtId));
      counter.incrementAndGet();
      queueCustomers(queue, counter, warehouseId, districtId, date);
      queueOrders(queue, counter, warehouseId, districtId, date);
    }
  }

  private void queueCustomers(BlockingQueue<TPCCRecord> queue, AtomicInteger counter,
      int warehouseId, int districtId, Date date) throws InterruptedException {
    for (int customerId = 1; customerId <= District.CUSTOMERS; customerId++) {
      Customer customer = new Customer(warehouseId, districtId, customerId, date);
      String last = customer.getLastName();
      String first = customer.getFirstName();
      // customer
      queue.put(customer);
      counter.incrementAndGet();
      // customer_secondary
      queue.put(new CustomerSecondary(warehouseId, districtId, last, first, customerId));
      counter.incrementAndGet();
      // history
      queue.put(new History(customerId, districtId, warehouseId, districtId, warehouseId, date));
      counter.incrementAndGet();
    }
  }

  private void queueOrders(BlockingQueue<TPCCRecord> queue, AtomicInteger counter, int warehouseId,
      int districtId, Date date) throws InterruptedException {
    List<Integer> customers = new ArrayList<Integer>();
    for (int customerId = 1; customerId <= District.CUSTOMERS; customerId++) {
      customers.add(customerId);
    }
    Collections.shuffle(customers);
    Integer[] permutation = customers.toArray(new Integer[District.CUSTOMERS]);

    for (int orderId = 1; orderId <= District.ORDERS; orderId++) {
      int customerId = permutation[orderId - 1];
      Order order = new Order(warehouseId, districtId, orderId, customerId, date);
      int orderLineCount = order.getOrderLineCount();

      // order
      queue.put(order);
      counter.incrementAndGet();
      for (int number = 1; number <= orderLineCount; number++) {
        int itemId = TPCCUtil.randomInt(1, Item.ITEMS);
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

  private void load(DistributedTransactionManager manager) throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(numThreads + 1);
    BlockingQueue<TPCCRecord> queue = new ArrayBlockingQueue<>(10000);
    AtomicBoolean isAllQueued = new AtomicBoolean();
    AtomicInteger queuedCounter = new AtomicInteger();
    AtomicInteger succeededCounter = new AtomicInteger();
    AtomicInteger failedCounter = new AtomicInteger();

    for (int i = 0; i < numThreads; ++i) {
      executor.execute(() -> {
        while (true) {
          TPCCRecord record = queue.poll();
          if (record == null) {
            if (isAllQueued.get()) {
              break;
            }
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            continue;
          }
          try {
            record.insert(manager);
            succeededCounter.incrementAndGet();
          } catch (Exception e) {
            e.printStackTrace();
            failedCounter.incrementAndGet();
          }
        }
      });
    }

    Future<?> future = executor.submit(() -> {
      while (!isAllQueued.get()
          || succeededCounter.get() + failedCounter.get() < queuedCounter.get()) {
        System.out.println(succeededCounter.get() + " succeeded, " + failedCounter + " failed");
        Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      }
    });

    if (directory != null) {
      queueCSV(new File(directory, warehouse), queue, queuedCounter);
      queueCSV(new File(directory, item), queue, queuedCounter);
      queueCSV(new File(directory, stock), queue, queuedCounter);
      queueCSV(new File(directory, district), queue, queuedCounter);
      queueCSV(new File(directory, customer), queue, queuedCounter);
      queueCSV(new File(directory, customerSecondary), queue, queuedCounter);
      queueCSV(new File(directory, history), queue, queuedCounter);
      queueCSV(new File(directory, order), queue, queuedCounter);
      queueCSV(new File(directory, newOrder), queue, queuedCounter);
      queueCSV(new File(directory, orderLine), queue, queuedCounter);
    } else {
      for (int itemId = 1; itemId <= Item.ITEMS; itemId++) {
        queue.put(new Item(itemId));
        queuedCounter.incrementAndGet();
      }
      queueWarehouses(queue, queuedCounter);
    }
    isAllQueued.set(true);

    try {
      future.get();
    } catch (java.util.concurrent.ExecutionException e) {
      e.printStackTrace();
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
  }

  private void queueCSV(File file, BlockingQueue<TPCCRecord> queue, AtomicInteger counter)
      throws InterruptedException {

    CSVFormat format = CSVFormat.Builder.create(CSVFormat.DEFAULT)
        .setHeader(headerMap.get(file.getName())).build();

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(new BOMInputStream(new FileInputStream(file))))) {
      CSVParser parser = CSVParser.parse(reader, format);
      for (CSVRecord record : parser) {
        switch (file.getName()) {
          case customer:
            queue.put(new Customer(record));
            break;
          case customerSecondary:
            queue.put(new CustomerSecondary(record));
            break;
          case district:
            queue.put(new District(record));
            break;
          case history:
            queue.put(new History(record));
            break;
          case item:
            queue.put(new Item(record));
            break;
          case newOrder:
            queue.put(new NewOrder(record));
            break;
          case order:
            queue.put(new Order(record));
            break;
          case orderLine:
            queue.put(new OrderLine(record));
            break;
          case stock:
            queue.put(new Stock(record));
            break;
          case warehouse:
            queue.put(new Warehouse(record));
            break;
          default:
        }
        counter.incrementAndGet();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
