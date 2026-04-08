package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.CONFIG_NAME;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.OPS_PER_TX;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordCount;
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

/** Workload F: Read-modify-write. */
public class WorkloadF extends TimeBasedProcessor {
  // one read-modify-write operation (one read and one write for the same record is regarded as one
  // operation)
  private static final long DEFAULT_OPS_PER_TX = 1;
  private final DistributedTransactionManager manager;
  private final int recordCount;
  private final int recordsPerPartition;
  private final int opsPerTx;
  private final int payloadSize;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadF(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);
    this.recordCount = getRecordCount(config);
    this.recordsPerPartition = getRecordsPerPartition(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    this.payloadSize = getPayloadSize(config);
  }

  @Override
  public void executeEach() throws TransactionException {
    List<Integer> userIds = new ArrayList<>(opsPerTx);
    List<Integer> seqs = new ArrayList<>(opsPerTx);
    List<byte[]> payloads = new ArrayList<>(opsPerTx);
    byte[] payload = new byte[payloadSize];
    for (int i = 0; i < opsPerTx; ++i) {
      userIds.add(ThreadLocalRandom.current().nextInt(recordCount));
      seqs.add(ThreadLocalRandom.current().nextInt(recordsPerPartition));

      YcsbCommon.randomFastBytes(ThreadLocalRandom.current(), payload);
      payloads.add(payload.clone());
    }

    while (true) {
      DistributedTransaction transaction = manager.start();
      try {
        for (int i = 0; i < userIds.size(); i++) {
          int userId = userIds.get(i);
          int seq = seqs.get(i);
          transaction.get(prepareGet(userId, seq));
          transaction.put(preparePut(userId, seq, payloads.get(i)));
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
