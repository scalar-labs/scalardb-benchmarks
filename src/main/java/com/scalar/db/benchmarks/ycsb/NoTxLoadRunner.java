package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_PRIMARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_SECONDARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadBatchSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadConcurrency;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadOverwrite;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareGet;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.preparePut;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.randomFastChars;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.benchmarks.Common;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.kelpie.config.Config;
import io.github.resilience4j.retry.Retry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoTxLoadRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoTxLoadRunner.class);
  // private final DistributedTransactionManager manager;
  private final DistributedStorage storage;
  private final int id;
  private final int concurrency;
  private final int recordCount;
  private final char[] payload;
  private final int batchSize;
  private final boolean overwrite;

  public NoTxLoadRunner(Config config, DistributedStorage storage, int threadId) {
    this.id = threadId;
    // this.manager = manager;
    this.storage = storage;
    concurrency = getLoadConcurrency(config);
    batchSize = getLoadBatchSize(config);
    recordCount = getRecordCount(config);
    payload = new char[getPayloadSize(config)];
    overwrite = getLoadOverwrite(config);
  }

  public void run() {
    run(false);
  }

  public void runForMultiStorage() {
    run(true);
  }

  private void run(boolean forMultiStorage) {
    int numPerThread = (recordCount + concurrency - 1) / concurrency;
    int start = numPerThread * id;
    int end = Math.min(numPerThread * (id + 1), recordCount);
    IntStream.range(0, (numPerThread + batchSize - 1) / batchSize)
        .forEach(
            i -> {
              int startId = start + batchSize * i;
              int endId = Math.min(start + batchSize * (i + 1), end);
              populateWithTx(startId, endId, forMultiStorage);
            });
  }

  private void populateWithTx(int startId, int endId, boolean forMultiStorage) {
    Runnable populate =
        () -> {
          // DistributedTransaction transaction = null;
          try {
            // transaction = manager.start();
            for (int i = startId; i < endId; ++i) {
              randomFastChars(ThreadLocalRandom.current(), payload);
              if (forMultiStorage) {
                putForMultiStorage(storage, i, new String(payload));
              } else {
                putForSingleStorage(storage, i, new String(payload));
              }
            }
            // transaction.commit();
          } catch (Exception e) {
            /*
            if (transaction != null) {
              try {
                transaction.abort();
              } catch (AbortException ex) {
                LOGGER.warn("Abort failed", ex);
              }
            }
            */
            LOGGER.warn("Load failed", e);
            throw new RuntimeException("Load failed", e);
          }
        };

    Retry retry = Common.getRetryWithFixedWaitDuration("load");
    Runnable decorated = Retry.decorateRunnable(retry, populate);
    try {
      decorated.run();
    } catch (Exception e) {
      LOGGER.error("Load failed repeatedly!");
      throw e;
    }
  }

  private void putForSingleStorage(DistributedStorage storage, int userId, String payload)
      throws ExecutionException {
    if (overwrite) {
      Get get = prepareGet(userId);
      storage.get(get);
    }
    Put put = preparePut(userId, payload);
    storage.put(put);
  }

  private void putForMultiStorage(DistributedStorage storage, int userId, String payload)
      throws ExecutionException {
    if (overwrite) {
      Get primaryGet = prepareGet(NAMESPACE_PRIMARY, userId);
      Get secondaryGet = prepareGet(NAMESPACE_SECONDARY, userId);
      storage.get(primaryGet);
      storage.get(secondaryGet);
    }
    Put primaryPut = preparePut(NAMESPACE_PRIMARY, userId, payload);
    Put secondaryPut = preparePut(NAMESPACE_SECONDARY, userId, payload);
    storage.put(primaryPut);
    storage.put(secondaryPut);
  }
}
