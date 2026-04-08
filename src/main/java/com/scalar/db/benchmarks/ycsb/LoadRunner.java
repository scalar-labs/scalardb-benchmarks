package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_PRIMARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_SECONDARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadBatchSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadConcurrency;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadOverwrite;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPartitionCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordsPerPartition;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareGet;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.preparePut;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.randomFastBytes;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.benchmarks.Common;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.kelpie.config.Config;
import io.github.resilience4j.retry.Retry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadRunner.class);
  private final DistributedTransactionManager manager;
  private final int id;
  private final int concurrency;
  private final int partitionCount;
  private final int recordsPerPartition;
  private final byte[] payload;
  private final int batchSize;
  private final boolean overwrite;

  public LoadRunner(Config config, DistributedTransactionManager manager, int threadId) {
    this.id = threadId;
    this.manager = manager;
    concurrency = getLoadConcurrency(config);
    batchSize = getLoadBatchSize(config);
    partitionCount = getPartitionCount(config);
    recordsPerPartition = getRecordsPerPartition(config);
    payload = new byte[getPayloadSize(config)];
    overwrite = getLoadOverwrite(config);
  }

  public void run() {
    run(false);
  }

  public void runForMultiStorage() {
    run(true);
  }

  private void run(boolean forMultiStorage) {
    int numPerThread = (partitionCount + concurrency - 1) / concurrency;
    int start = numPerThread * id;
    int end = Math.min(numPerThread * (id + 1), partitionCount);
    IntStream.range(start, end).forEach(partitionKey -> populatePartition(partitionKey, forMultiStorage));
  }

  private void populatePartition(int partitionKey, boolean forMultiStorage) {
    Runnable populate =
        () -> {
          DistributedTransaction transaction = null;
          try {
            transaction = manager.start();
            for (int seq = 0; seq < recordsPerPartition; seq++) {
              randomFastBytes(ThreadLocalRandom.current(), payload);
              if (forMultiStorage) {
                putForMultiStorage(transaction, partitionKey, seq, payload.clone());
              } else {
                putForSingleStorage(transaction, partitionKey, seq, payload.clone());
              }
            }
            transaction.commit();
          } catch (Exception e) {
            if (transaction != null) {
              try {
                transaction.abort();
              } catch (AbortException ex) {
                LOGGER.warn("Abort failed", ex);
              }
            }
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

  private void putForSingleStorage(
      DistributedTransaction transaction, int userId, int seq, byte[] payload)
      throws TransactionException {
    if (overwrite) {
      Get get = prepareGet(userId, seq);
      transaction.get(get);
    }
    Put put = preparePut(userId, seq, payload);
    transaction.put(put);
  }

  private void putForMultiStorage(
      DistributedTransaction transaction, int userId, int seq, byte[] payload)
      throws TransactionException {
    if (overwrite) {
      Get primaryGet = prepareGet(NAMESPACE_PRIMARY, userId, seq);
      Get secondaryGet = prepareGet(NAMESPACE_SECONDARY, userId, seq);
      transaction.get(primaryGet);
      transaction.get(secondaryGet);
    }
    Put primaryPut = preparePut(NAMESPACE_PRIMARY, userId, seq, payload);
    Put secondaryPut = preparePut(NAMESPACE_SECONDARY, userId, seq, payload);
    transaction.put(primaryPut);
    transaction.put(secondaryPut);
  }
}
