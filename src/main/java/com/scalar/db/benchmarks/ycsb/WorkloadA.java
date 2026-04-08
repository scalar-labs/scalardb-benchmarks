package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.CONFIG_NAME;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.OPS_PER_TX;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPartitionCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordsPerPartition;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareGet;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.preparePut;

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
 * Workload A: Update heavy workload. This workload has a mix of 50/50 reads and writes. The writes
 * in the original Workload A are blind writes. However, ScalarDB doesn't allow a blind write for an
 * existing record when you're using the default transaction manager, Consensus Commit. So, we use
 * read-modify-write operations instead. You can change them to the blind writes by setting
 * "use_read_modify_write" to false.
 */
public class WorkloadA extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 2; // one read operation and one write operation
  private static final String USE_READ_MODIFY_WRITE = "use_read_modify_write";
  private final DistributedTransactionManager manager;
  private final int partitionCount;
  private final int recordsPerPartition;
  private final int opsPerTx;
  private final boolean useReadModifyWrite;
  private final int payloadSize;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadA(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);
    this.partitionCount = getPartitionCount(config);
    this.recordsPerPartition = getRecordsPerPartition(config);
    this.payloadSize = getPayloadSize(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    if (opsPerTx % 2 != 0) {
      throw new IllegalArgumentException(OPS_PER_TX + " must be a multiple of 2.");
    }
    useReadModifyWrite = config.getUserBoolean(CONFIG_NAME, USE_READ_MODIFY_WRITE, true);
  }

  @Override
  public void executeEach() throws TransactionException {
    int readOpsPerTx = opsPerTx / 2;
    int writeOpsPerTx = opsPerTx / 2;

    List<Integer> readUserIds = new ArrayList<>(readOpsPerTx);
    List<Integer> readSeqs = new ArrayList<>(readOpsPerTx);
    for (int i = 0; i < readOpsPerTx; ++i) {
      readUserIds.add(ThreadLocalRandom.current().nextInt(partitionCount));
      readSeqs.add(ThreadLocalRandom.current().nextInt(recordsPerPartition));
    }

    List<Integer> writeUserIds = new ArrayList<>(writeOpsPerTx);
    List<Integer> writeSeqs = new ArrayList<>(writeOpsPerTx);
    List<byte[]> payloads = new ArrayList<>(writeOpsPerTx);
    byte[] payload = new byte[payloadSize];
    for (int i = 0; i < writeOpsPerTx; ++i) {
      writeUserIds.add(ThreadLocalRandom.current().nextInt(partitionCount));
      writeSeqs.add(ThreadLocalRandom.current().nextInt(recordsPerPartition));

      YcsbCommon.randomFastBytes(ThreadLocalRandom.current(), payload);
      payloads.add(payload.clone());
    }

    while (true) {
      DistributedTransaction transaction = manager.start();
      try {
        for (int i = 0; i < readUserIds.size(); i++) {
          transaction.get(prepareGet(readUserIds.get(i), readSeqs.get(i)));
        }

        for (int i = 0; i < writeUserIds.size(); i++) {
          int writeUserId = writeUserIds.get(i);
          int writeSeq = writeSeqs.get(i);
          if (useReadModifyWrite) {
            transaction.get(prepareGet(writeUserId, writeSeq));
          }
          transaction.put(preparePut(writeUserId, writeSeq, payloads.get(i)));
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
