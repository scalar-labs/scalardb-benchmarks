package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.CONFIG_NAME;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_PRIMARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE_SECONDARY;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.OPS_PER_TX;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareGet;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.preparePut;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Mutation;
import com.scalar.db.benchmarks.Common;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import javax.json.Json;

/**
 * Multi-storage workload Fe: Same number of read-modify-write operation for both primary and
 * secondary database.
 */
public class NoTxMultiStorageWorkloadF extends TimeBasedProcessor {
  // one read-modify-write operation (one read and one write for the same record is regarded as one
  // operation)
  private static final long DEFAULT_OPS_PER_TX = 1;
  // private final DistributedTransactionManager manager;
  private final DistributedStorage storage;
  private final int recordCount;
  private final int opsPerTx;
  private final int payloadSize;
  ExecutorService service = Executors.newFixedThreadPool(256);

  public NoTxMultiStorageWorkloadF(Config config) {
    super(config);
    StorageFactory factory =
        StorageFactory.create(Common.getDatabaseConfig(config).getProperties());
    storage = factory.getStorage();
    // this.manager = Common.getTransactionManager(config);
    this.recordCount = getRecordCount(config);
    this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
    this.payloadSize = getPayloadSize(config);
  }

  @Override
  public void executeEach() throws Exception {
    List<Integer> primaryIds = new ArrayList<>(opsPerTx);
    List<Integer> secondaryIds = new ArrayList<>(opsPerTx);
    List<String> payloads = new ArrayList<>(opsPerTx);
    char[] payload = new char[payloadSize];
    for (int i = 0; i < opsPerTx; ++i) {
      primaryIds.add(ThreadLocalRandom.current().nextInt(recordCount));
      secondaryIds.add(ThreadLocalRandom.current().nextInt(recordCount));

      YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
      payloads.add(new String(payload)); // use same payload for primary and secondary
    }

    while (true) {
      // DistributedTransaction transaction = manager.start();
      try {
        List<Mutation> writeOps1 = new ArrayList<>();
        for (int i = 0; i < primaryIds.size(); i++) {
          int userId = primaryIds.get(i);
          storage.get(prepareGet(NAMESPACE_PRIMARY, userId));
          writeOps1.add(preparePut(NAMESPACE_PRIMARY, userId, payloads.get(i)));
          // manager.put(preparePut(NAMESPACE_PRIMARY, userId, payloads.get(i)));
        }

        List<Mutation> writeOps2 = new ArrayList<>();
        for (int i = 0; i < secondaryIds.size(); i++) {
          int userId = secondaryIds.get(i);
          storage.get(prepareGet(NAMESPACE_SECONDARY, userId));
          writeOps2.add(preparePut(NAMESPACE_SECONDARY, userId, payloads.get(i)));
          // manager.put(preparePut(NAMESPACE_SECONDARY, userId, payloads.get(i)));
        }
        Future<?> future1 =
            service.submit(
                () -> {
                  try {
                    storage.mutate(writeOps1);
                  } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                  }
                });
        Future<?> future2 =
            service.submit(
                () -> {
                  try {
                    storage.mutate(writeOps2);
                  } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                  }
                });
        // transaction.commit();
        future1.get();
        future2.get();
        break;
      } catch (Exception e) {
        throw e;
      }
    }
  }

  @Override
  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      logWarn("Failed to close the transaction manager", e);
    }

    setState(Json.createObjectBuilder().add("transaction-retry-count", "0").build());
  }
}
