package com.scalar.db.benchmarks.tpcc;

import com.scalar.db.api.DistributedTransactionManager;
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
      required = false,
      paramLabel = "NUM_WAREHOUSE",
      description = "The number of warehouse.")
  private int numWarehouse = 1;

  @CommandLine.Option(
      names = {"--rate-new-order"},
      required = false,
      paramLabel = "RATE_NEW_ORDER",
      description = "The percentage of new-order transaction.")
  private int rateNewOrder = 50;

  @CommandLine.Option(
      names = {"--rate-payment"},
      required = false,
      paramLabel = "RATE_PAYMENT",
      description = "The percentage of payment transaction.")
  private int ratePayment = 50;

  @CommandLine.Option(
      names = {"--rate-order-status"},
      required = false,
      paramLabel = "RATE_ORDER_STATUS",
      description = "The percentage of order-status transaction.")
  private int rateOrderStatus = 0;

  @CommandLine.Option(
      names = {"--rate-delivery"},
      required = false,
      paramLabel = "RATE_DERIVERRY",
      description = "The percentage of delivery transaction.")
  private int rateDelivery = 0;

  @CommandLine.Option(
      names = {"--rate-stock-level"},
      required = false,
      paramLabel = "RATE_STOCK_LEVEL",
      description = "The percentage of stock-level transaction.")
  private int rateStockLevel = 0;

  @CommandLine.Option(
      names = {"--num-threads"},
      required = false,
      paramLabel = "NUM_THREADS",
      description = "The number of threads to run.")
  private int numThreads = 1;

  @CommandLine.Option(
      names = {"--duration"},
      required = false,
      paramLabel = "DURATION",
      description = "The duration of benchmark in seconds")
  private int duration = 200;

  @CommandLine.Option(
      names = {"--ramp-up-time"},
      required = false,
      paramLabel = "RAMP_UP_TIME",
      description = "The ramp up time in seconds.")
  private int rampUpTime = 60;

  @CommandLine.Option(
      names = {"--times"},
      required = false,
      paramLabel = "TIMES",
      description = "The number of serial execution for testing.")
  private int times = 0;

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "display the help message.")
  boolean helpRequested;

  private static final String namespace = "tpcc";
  private static final AtomicInteger counter = new AtomicInteger();
  private static final AtomicInteger totalCounter = new AtomicInteger();
  private static final AtomicLong latencyTotal = new AtomicLong();
  private static final AtomicInteger errorCounter = new AtomicInteger();

  public static void main(String[] args) {
    int exitCode = new CommandLine(new TpccBench()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    DatabaseConfig dbConfig = new DatabaseConfig(new FileInputStream(properties));
    TransactionFactory factory = new TransactionFactory(dbConfig);
    DistributedTransactionManager manager = factory.getTransactionManager();
    manager.withNamespace(namespace);
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
      System.exit(0);
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
            // e.printStackTrace();
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

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore
      }
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
