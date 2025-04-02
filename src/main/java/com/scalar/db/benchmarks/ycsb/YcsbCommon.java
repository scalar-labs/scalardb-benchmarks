package com.scalar.db.benchmarks.ycsb;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
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
  static final String YCSB_CLUSTERING_KEY = "ycsb_clustering_key";
  static final String PAYLOAD = "payload";
  static final String CONFIG_NAME = "ycsb_config";
  static final String LOAD_CONCURRENCY = "load_concurrency";
  static final String LOAD_BATCH_SIZE = "load_batch_size";
  static final String LOAD_OVERWRITE = "load_overwrite";
  static final String RECORD_COUNT = "record_count";
  static final String PAYLOAD_SIZE = "payload_size";
  static final String OPS_PER_TX = "ops_per_tx";
  private static final int CHAR_START = 32; // [space]
  private static final int CHAR_STOP = 126; // [~]
  private static final char[] CHAR_SYMBOLS = new char[1 + CHAR_STOP - CHAR_START];
  private static final int[] FAST_MASKS = {
    554189328, // 10000
    277094664, // 01000
    138547332, // 00100
    69273666, // 00010
    34636833, // 00001
    346368330, // 01010
    727373493, // 10101
    588826161, // 10001
    935194491, // 11011
    658099827, // 10011
  };

  static {
    for (int i = 0; i < CHAR_SYMBOLS.length; i++) {
      CHAR_SYMBOLS[i] = (char) (CHAR_START + i);
    }
  }

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
        .partitionKey(Key.ofInt(YCSB_KEY, 0))
        .clusteringKey(Key.ofInt(YCSB_CLUSTERING_KEY, key))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  public static Scan prepareScan() {
    return prepareScan(NAMESPACE, TABLE);
  }

  public static Scan prepareScan(String namespace) {
    return prepareScan(namespace, TABLE);
  }

  public static Scan prepareScan(String namespace, String table) {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(YCSB_KEY, 0))
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  public static Scan prepareScanAll() {
    return prepareScanAll(NAMESPACE, TABLE);
  }

  public static Scan prepareScanAll(String namespace) {
    return prepareScanAll(namespace, TABLE);
  }

  public static Scan prepareScanAll(String namespace, String table) {
    return Scan.newBuilder()
        .namespace(namespace)
        .table(table)
        .all()
        .consistency(Consistency.LINEARIZABLE)
        .build();
  }

  public static Put preparePut(int key, String payload) {
    return preparePut(NAMESPACE, TABLE, key, payload);
  }

  public static Put preparePut(String namespace, int key, String payload) {
    return preparePut(namespace, TABLE, key, payload);
  }

  public static Put preparePut(String namespace, String table, int key, String payload) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(table)
        .partitionKey(Key.ofInt(YCSB_KEY, 0))
        .clusteringKey(Key.ofInt(YCSB_CLUSTERING_KEY, key))
        .value(TextColumn.of(PAYLOAD, payload))
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

  // This method is taken from benchbase.
  // https://github.com/cmu-db/benchbase/blob/bbe8c1db84ec81c6cdec6fbeca27b24b1b4e6612/src/main/java/com/oltpbenchmark/util/TextGenerator.java#L80
  public static char[] randomFastChars(Random rng, char[] chars) {
    // Ok so now the goal of this is to reduce the number of times that we have to
    // invoke a random number. We'll do this by grabbing a single random int
    // and then taking different bitmasks

    int num_rounds = chars.length / FAST_MASKS.length;
    int i = 0;
    for (int ctr = 0; ctr < num_rounds; ctr++) {
      int rand = rng.nextInt(10000); // CHAR_SYMBOLS.length);
      for (int mask : FAST_MASKS) {
        chars[i++] = CHAR_SYMBOLS[(rand | mask) % CHAR_SYMBOLS.length];
      }
    }
    // Use the old way for the remaining characters
    // I am doing this because I am too lazy to think of something more clever
    for (; i < chars.length; i++) {
      chars[i] = CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
    }
    return (chars);
  }
}
