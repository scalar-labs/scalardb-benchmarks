package com.scalar.db.benchmarks.tpcc;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.table.TpccRecord;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.TransactionFactory;
import java.io.FileInputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "tpcc-bench", description = "Execute TPC-C benchmark.")
public class TpccBench implements Callable<Integer> {

  @CommandLine.Option(
      names = {"--properties", "--config"},
      required = true,
      paramLabel = "PROPERTIES_FILE",
      description = "A configuration file in properties format.")
  private String properties;

  @CommandLine.Option(
      names = {"--num-warehouse"},
      paramLabel = "NUM_WAREHOUSE",
      defaultValue = "1",
      description = "The number of warehouse.")
  private int numWarehouse;

  @CommandLine.Option(
      names = {"--rate-new-order"},
      paramLabel = "RATE_NEW_ORDER",
      defaultValue = "50",
      description = "The percentage of new-order transaction.")
  private int rateNewOrder;

  @CommandLine.Option(
      names = {"--rate-payment"},
      paramLabel = "RATE_PAYMENT",
      defaultValue = "50",
      description = "The percentage of payment transaction.")
  private int ratePayment;

  @CommandLine.Option(
      names = {"--rate-order-status"},
      paramLabel = "RATE_ORDER_STATUS",
      defaultValue = "0",
      description = "The percentage of order-status transaction.")
  private int rateOrderStatus;

  @CommandLine.Option(
      names = {"--rate-delivery"},
      paramLabel = "RATE_DELIVERY",
      defaultValue = "0",
      description = "The percentage of delivery transaction.")
  private int rateDelivery;

  @CommandLine.Option(
      names = {"--rate-stock-level"},
      paramLabel = "RATE_STOCK_LEVEL",
      defaultValue = "0",
      description = "The percentage of stock-level transaction.")
  private int rateStockLevel;

  @CommandLine.Option(
      names = {"--num-threads"},
      paramLabel = "NUM_THREADS",
      defaultValue = "1",
      description = "The number of threads to run.")
  private int numThreads;

  @CommandLine.Option(
      names = {"--duration"},
      paramLabel = "DURATION",
      defaultValue = "200",
      description = "The duration of benchmark in seconds")
  private int duration;

  @CommandLine.Option(
      names = {"--ramp-up-time"},
      paramLabel = "RAMP_UP_TIME",
      defaultValue = "60",
      description = "The ramp up time in seconds.")
  private int rampUpTime;

  @CommandLine.Option(
      names = {"--times"},
      paramLabel = "TIMES",
      defaultValue = "0",
      description = "The number of serial execution for testing.")
  private int times;

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "display the help message.")
  private boolean helpRequested;

  private final AtomicInteger counter = new AtomicInteger();
  private final AtomicInteger totalCounter = new AtomicInteger();
  private final AtomicLong latencyTotal = new AtomicLong();
  private final AtomicInteger errorCounter = new AtomicInteger();

  public static void main(String[] args) {
    int exitCode = new CommandLine(new TpccBench()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    DatabaseConfig dbConfig = new DatabaseConfig(new FileInputStream(properties));
    TransactionFactory factory = new TransactionFactory(dbConfig);
    DistributedTransactionManager manager = factory.getTransactionManager();
    manager.withNamespace(TpccRecord.NAMESPACE);
    TpccConfig config = TpccConfig.newBuilder()
        .numWarehouse(numWarehouse)
        .rateNewOrder(rateNewOrder)
        .ratePayment(ratePayment)
        .rateOrderStatus(rateOrderStatus)
        .rateDelivery(rateDelivery)
        .rateStockLevel(rateStockLevel)
        .build();

    if (times > 0) {
      TpccRunner tpcc = new TpccRunner(manager, config);
      for (int i = 0; i < times; ++i) {
        try {
          tpcc.run();
        } catch (TransactionException e) {
          e.printStackTrace();
        }
      }
      return 0;
    }

    long durationMillis = duration * 1000L;
    long rampUpTimeMillis = rampUpTime * 1000L;

    AtomicBoolean isRunning = new AtomicBoolean(true);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final long start = System.currentTimeMillis();
    long from = start;
    for (int i = 0; i < numThreads; ++i) {
      executor.execute(() -> {
        TpccRunner tpcc = new TpccRunner(manager, config);
        while (isRunning.get()) {
          try {
            long eachStart = System.currentTimeMillis();
            tpcc.run();
            long eachEnd = System.currentTimeMillis();
            counter.incrementAndGet();
            if (System.currentTimeMillis() >= start + rampUpTimeMillis) {
              totalCounter.incrementAndGet();
              latencyTotal.addAndGet(eachEnd - eachStart);
            }
          } catch (TransactionException e) {
            errorCounter.incrementAndGet();
          }
        }
      });
    }

    long end = start + rampUpTimeMillis + durationMillis;
    while (true) {
      long to = System.currentTimeMillis();
      if (to >= end) {
        isRunning.set(false);
        break;
      }
      System.out.println(((double) counter.get() * 1000 / (to - from)) + " tps");
      counter.set(0);
      from = System.currentTimeMillis();

      Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
    }
    System.out
        .println("TPS: " + (double) totalCounter.get() * 1000 / (end - start - rampUpTimeMillis));
    System.out.println("Average-Latency(ms): " + (double) latencyTotal.get() / totalCounter.get());
    System.out.println("Error-Counts: " + errorCounter.get());

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    manager.close();

    return 0;
  }
}
