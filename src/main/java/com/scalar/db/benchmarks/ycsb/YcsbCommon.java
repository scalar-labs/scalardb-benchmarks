package com.scalar.db.benchmarks.ycsb;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.Key;
import com.scalar.kelpie.config.Config;
import java.util.Random;

public class YcsbCommon {
  static final long DEFAULT_LOAD_CONCURRENCY = 1;
  static final long DEFAULT_LOAD_BATCH_SIZE = 1;
  static final long DEFAULT_RECORD_COUNT = 1000;
  static final long DEFAULT_PAYLOAD_SIZE = 1000;
  static final String NAMESPACE = "ycsb";
  static final String NAMESPACE_PRIMARY = "ycsb_primary"; // for multi-storage mode
  static final String NAMESPACE_SECONDARY = "ycsb_secondary"; // for multi-storage mode
  static final String TABLE = "usertable";
  static final String YCSB_KEY = "ycsb_key";
  static final String PAYLOAD = "payload";
  static final String CONFIG_NAME = "ycsb_config";
  static final String LOAD_CONCURRENCY = "load_concurrency";
  static final String LOAD_BATCH_SIZE = "load_batch_size";
  static final String LOAD_OVERWRITE = "load_overwrite";
  static final String RECORD_COUNT = "record_count";
  static final String PAYLOAD_SIZE = "payload_size";
  static final String OPS_PER_TX = "ops_per_tx";

  public static Get prepareGet(int key) {
    return prepareGet(NAMESPACE, TABLE, key);
  }

  public static Get prepareGet(String namespace, int key) {
    return prepareGet(namespace, TABLE, key);
  }

  public static Get prepareGet(String namespace, String table, int key) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(YCSB_KEY, key))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  public static Put preparePut(int key, byte[] payload) {
    return preparePut(NAMESPACE, TABLE, key, payload);
  }

  public static Put preparePut(String namespace, int key, byte[] payload) {
    return preparePut(namespace, TABLE, key, payload);
  }

  public static Put preparePut(String namespace, String table, int key, byte[] payload) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(YCSB_KEY, key))
        .value(BlobColumn.of(PAYLOAD, payload))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  public static int getLoadConcurrency(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, LOAD_CONCURRENCY, DEFAULT_LOAD_CONCURRENCY);
  }

  public static int getLoadBatchSize(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, LOAD_BATCH_SIZE, DEFAULT_LOAD_BATCH_SIZE);
  }

  public static boolean getLoadOverwrite(Config config) {
    return config.getUserBoolean(CONFIG_NAME, LOAD_OVERWRITE, false);
  }

  public static int getRecordCount(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, RECORD_COUNT, DEFAULT_RECORD_COUNT);
  }

  public static int getPayloadSize(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, PAYLOAD_SIZE, DEFAULT_PAYLOAD_SIZE);
  }

  public static byte[] randomFastBytes(Random rng, byte[] bytes) {
    rng.nextBytes(bytes);
    return bytes;
  }
}
