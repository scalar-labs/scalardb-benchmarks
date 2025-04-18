package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_PRIMARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_SECONDARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadBatchSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadConcurrency;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadOverwrite;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareGet;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareObjectKey;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.preparePut;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.randomFastChars;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.benchmarks.Common;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapper;
import com.scalar.kelpie.config.Config;
import io.github.resilience4j.retry.Retry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadRunner.class);
  private final ObjectStorageWrapper wrapper;
  private final int id;
  private final int concurrency;
  private final int recordCount;
  private final char[] payload;
  private final int batchSize;
  private final boolean overwrite;

  public LoadRunner(Config config, DistributedTransactionManager manager, int threadId) {
    this.id = threadId;
    this.wrapper = Common.getObjectStorageWrapper(config);
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
          for (int i = startId; i < endId; ++i) {
            randomFastChars(ThreadLocalRandom.current(), payload);
            wrapper.upsert(prepareObjectKey(i), new String(payload));
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

  private void putForSingleStorage(DistributedTransaction transaction, int userId, String payload)
      throws TransactionException {
    if (overwrite) {
      Get get = prepareGet(userId);
      transaction.get(get);
    }
    Put put = preparePut(userId, payload);
    transaction.put(put);
  }

  private void putForMultiStorage(DistributedTransaction transaction, int userId, String payload)
      throws TransactionException {
    if (overwrite) {
      Get primaryGet = prepareGet(NAMESPACE_PRIMARY, userId);
      Get secondaryGet = prepareGet(NAMESPACE_SECONDARY, userId);
      transaction.get(primaryGet);
      transaction.get(secondaryGet);
    }
    Put primaryPut = preparePut(NAMESPACE_PRIMARY, userId, payload);
    Put secondaryPut = preparePut(NAMESPACE_SECONDARY, userId, payload);
    transaction.put(primaryPut);
    transaction.put(secondaryPut);
  }
}
