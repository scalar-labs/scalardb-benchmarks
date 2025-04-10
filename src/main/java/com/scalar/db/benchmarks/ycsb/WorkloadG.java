package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.CONFIG_NAME;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.OPS_PER_TX;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareObjectKey;

import com.scalar.db.benchmarks.Common;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapper;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.json.Json;

/**
 * Workload A: Update heavy workload. This workload has a mix of 50/50 reads and writes. The writes
 * can be changed to read-modify-write if "use_read_modify_write" is set to true.
 */
public class WorkloadG extends TimeBasedProcessor {
  private static final long DEFAULT_OPS_PER_TX = 2; // one read operation and one write operation
  private final ObjectStorageWrapper wrapper;
  private final int recordCount;
  private final int opsPerTx;
  private final int payloadSize;

  private final LongAdder transactionRetryCount = new LongAdder();

  public WorkloadG(Config config) {
    super(config);
    this.wrapper = Common.getObjectStorageWrapper(config);
    this.recordCount = getRecordCount(config);
    this.payloadSize = getPayloadSize(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
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

    for (int i = 0; i < userIds.size(); i++) {
      int userId = userIds.get(i);
      wrapper.upsert(prepareObjectKey(userId), payloads.get(i));
    }
  }

  @Override
  public void close() {
    try {
      wrapper.close();
    } catch (Exception e) {
      logWarn("Failed to close the transaction manager", e);
    }

    setState(
        Json.createObjectBuilder()
            .add("transaction-retry-count", transactionRetryCount.toString())
            .build());
  }
}
