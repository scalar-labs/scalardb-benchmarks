package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadConcurrency;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.Common;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class Loader extends PreProcessor {
  public static DistributedTransactionManager manager;
  private final int concurrency;

  public Loader(Config config) {
    super(config);
    synchronized (Loader.class) {
      if (manager == null) {
        manager = Common.getTransactionManager(config);
      }
    }
    concurrency = getLoadConcurrency(config);
  }

  @Override
  public void execute() {
    ExecutorService executorService = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                      () -> new LoadRunner(config, manager, i).run(), executorService);
              futures.add(future);
            });

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    logInfo("All records have been inserted");
  }

  @Override
  public void close() throws Exception {
//    manager.close();
  }
}
