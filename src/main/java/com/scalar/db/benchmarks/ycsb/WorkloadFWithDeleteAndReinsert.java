package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.CONFIG_NAME;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.OPS_PER_TX;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.TABLE;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.YCSB_KEY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareGet;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.preparePut;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.benchmarks.Common;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;

/** Extended Workload F: Read-modify-write & Delete-reinsert. */
public class WorkloadFWithDeleteAndReinsert extends TimeBasedProcessor {
  // one read-modify-write operation (one read and one write for the same record is regarded as one
  // operation)
  private static final long DEFAULT_OPS_PER_TX = 1;
  private final DistributedTransactionManager manager;
  private final int recordCount;
  private final int opsPerTx;
  private final int payloadSize;
  private final float ratioOfDeleteAndReinsert;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadFWithDeleteAndReinsert(Config config) {
    super(config);
    this.manager = Common.getTransactionManager(config);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    this.payloadSize = getPayloadSize(config);
    this.ratioOfDeleteAndReinsert =
        Float.parseFloat(config.getUserString(CONFIG_NAME, "ratio_of_reinsert", "0.2"));
  }

  @Override
  public void executeEach() throws TransactionException {
    List<Integer> userIds = new ArrayList<>(opsPerTx);
    List<String> payloads = new ArrayList<>(opsPerTx);
    char[] payload = new char[payloadSize];
    for (int i = 0; i < opsPerTx; ++i) {
      userIds.add(ThreadLocalRandom.current().nextInt(recordCount));

      YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
      payloads.add(new String(payload));
    }

    boolean isDelete =
        ThreadLocalRandom.current().nextFloat() < ratioOfDeleteAndReinsert;
    while (true) {
      DistributedTransaction transaction = manager.start();
      try {
        for (int i = 0; i < userIds.size(); i++) {
          int userId = userIds.get(i);
          transaction.get(prepareGet(userId));
          if (isDelete) {
            // Delete
            transaction.delete(prepareDelete(userId));
          }
          else {
            // Normal Read-Modify-Write or Reinsert
            switch (ThreadLocalRandom.current().nextInt(3)) {
              case 0:
                transaction.put(preparePut(userId, payloads.get(i)));
                break;
              case 1:
                transaction.put(preparePutOnlyWithPayload(userId, payloads.get(i)));
                break;
              case 2:
                transaction.put(preparePutOnlyWithAge(userId, ThreadLocalRandom.current().nextInt(100)));
                break;
              default:
                throw new AssertionError();
            }
          }
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

  public static Delete prepareDelete(int key) {
    return Delete.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(Key.ofInt(YCSB_KEY, key))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  public static Put preparePutOnlyWithPayload(int key, String payload) {
    return Put.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(Key.ofInt(YCSB_KEY, key))
        .value(TextColumn.of(YcsbCommon.PAYLOAD, payload))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  public static Put preparePutOnlyWithAge(int key, int age) {
    return Put.newBuilder()
        .namespace(NAMESPACE)
        .table(TABLE)
        .partitionKey(Key.ofInt(YCSB_KEY, key))
        .value(IntColumn.of(YcsbCommon.AGE, age))
        .consistency(Consistency.LINEARIZABLE)
        .build();
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
