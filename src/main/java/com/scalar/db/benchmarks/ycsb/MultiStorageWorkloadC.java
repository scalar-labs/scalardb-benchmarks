package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.CONFIG_NAME;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_PRIMARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_SECONDARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.OPS_PER_TX;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPartitionCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordsPerPartition;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareGet;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.Common;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;

/**
 * Multi-storage workload Fe: Same number of read operation for both primary and secondary database.
 */
public class MultiStorageWorkloadC extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 2; // 2 read operations per database
  private final DistributedTransactionManager manager;
  private final int partitionCount;
  private final int recordsPerPartition;
  private final int opsPerTx;

  private final LongAdder transactionRetryCount = new LongAdder();

  public MultiStorageWorkloadC(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);
    this.partitionCount = getPartitionCount(config);
    this.recordsPerPartition = getRecordsPerPartition(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
  }

  @Override
  public void executeEach() throws TransactionException {
    List<Integer> primaryIds = new ArrayList<>(opsPerTx);
    List<Integer> primarySeqs = new ArrayList<>(opsPerTx);
    List<Integer> secondaryIds = new ArrayList<>(opsPerTx);
    List<Integer> secondarySeqs = new ArrayList<>(opsPerTx);
    for (int i = 0; i < opsPerTx; ++i) {
      primaryIds.add(ThreadLocalRandom.current().nextInt(partitionCount));
      primarySeqs.add(ThreadLocalRandom.current().nextInt(recordsPerPartition));
      secondaryIds.add(ThreadLocalRandom.current().nextInt(partitionCount));
      secondarySeqs.add(ThreadLocalRandom.current().nextInt(recordsPerPartition));
    }

    while (true) {
      DistributedTransaction transaction = manager.start();
      try {
        for (int i = 0; i < primaryIds.size(); i++) {
          transaction.get(prepareGet(NAMESPACE_PRIMARY, primaryIds.get(i), primarySeqs.get(i)));
        }
        for (int i = 0; i < secondaryIds.size(); i++) {
          transaction.get(prepareGet(NAMESPACE_SECONDARY, secondaryIds.get(i), secondarySeqs.get(i)));
        }
        transaction.commit();
        break;
      } catch (CrudConflictException | CommitConflictException e) {
        transaction.abort();
        transactionRetryCount.increment();
      } catch (Exception e) {
        transaction.abort();
        throw e;
      }
    }
  }

  @Override
  public void close() {
    try {
      manager.close();
    } catch (Exception e) {
      logWarn("Failed to close the transaction manager", e);
    }

    setState(
        Json.createObjectBuilder()
            .add("transaction-retry-count", transactionRetryCount.toString())
            .build());
  }
}
