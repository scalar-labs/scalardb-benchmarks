package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class Stock extends TpccRecord {

  public static final String TABLE_NAME = "stock";
  public static final String COLUMN_PREFIX = "s_";
  public static final String KEY_WAREHOUSE_ID = "s_w_id";
  public static final String KEY_ITEM_ID = "s_i_id";
  public static final String KEY_QUANTITY = "s_quantity";
  public static final String KEY_YTD = "s_ytd";
  public static final String KEY_ORDER_CNT = "s_order_cnt";
  public static final String KEY_REMOTE_CNT = "s_remote_cnt";
  public static final String KEY_DATA = "s_data";
  public static final String KEY_DIST_PREFIX = "s_dist_";
  public static final String KEY_DISTRICT01 = "s_dist_01";
  public static final String KEY_DISTRICT02 = "s_dist_02";
  public static final String KEY_DISTRICT03 = "s_dist_03";
  public static final String KEY_DISTRICT04 = "s_dist_04";
  public static final String KEY_DISTRICT05 = "s_dist_05";
  public static final String KEY_DISTRICT06 = "s_dist_06";
  public static final String KEY_DISTRICT07 = "s_dist_07";
  public static final String KEY_DISTRICT08 = "s_dist_08";
  public static final String KEY_DISTRICT09 = "s_dist_09";
  public static final String KEY_DISTRICT10 = "s_dist_10";

  public static final int MIN_DATA = 26;
  public static final int MAX_DATA = 50;
  public static final int DIST_SIZE = 24;

  /**
   * Constructs a {@code Stock} with specified parameters for update.
   *
   * @param warehouseId a warehouse ID
   * @param itemId      an item ID
   * @param quantity    quantity in stock
   * @param ytd         a YTD balance
   * @param orderCount  number of order count
   * @param remoteCount number of remote count
   */
  public Stock(int warehouseId, int itemId, int quantity, double ytd, int orderCount,
      int remoteCount) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_ITEM_ID, itemId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_QUANTITY, quantity);
    valueMap.put(KEY_YTD, ytd);
    valueMap.put(KEY_ORDER_CNT, orderCount);
    valueMap.put(KEY_REMOTE_CNT, remoteCount);
  }

  /**
   * Constructs a {@code Stock} with data generation.
   *
   * @param warehouseId a warehouse ID
   * @param itemId      an item ID
   */
  public Stock(int warehouseId, int itemId) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_ITEM_ID, itemId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_QUANTITY, TpccUtil.randomInt(10, 100));
    valueMap.put(KEY_YTD, 0.00);
    valueMap.put(KEY_ORDER_CNT, 0);
    valueMap.put(KEY_REMOTE_CNT, 0);
    valueMap.put(KEY_DATA, TpccUtil.getRandomStringWithOriginal(MIN_DATA, MAX_DATA, 10));
    for (int i = 0; i < 10; i++) {
      String key = KEY_DIST_PREFIX + String.format("%02d", i + 1);
      valueMap.put(key, TpccUtil.randomAlphaString(DIST_SIZE));
    }
  }

  /**
   * Constructs a {@code Stock} with a CSV record.
   *
   * @param record a {@code CSVRecord} object
   */
  public Stock(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_ITEM_ID, Integer.parseInt(record.get(KEY_ITEM_ID)));

    valueMap = new HashMap<>();
    valueMap.put(KEY_QUANTITY, Integer.parseInt(record.get(KEY_QUANTITY)));
    valueMap.put(KEY_YTD, Double.parseDouble(record.get(KEY_YTD)));
    valueMap.put(KEY_ORDER_CNT, Integer.parseInt(record.get(KEY_ORDER_CNT)));
    valueMap.put(KEY_REMOTE_CNT, Integer.parseInt(record.get(KEY_REMOTE_CNT)));
    valueMap.put(KEY_DATA, record.get(KEY_DATA));
    for (int i = 0; i < 10; i++) {
      String key = KEY_DIST_PREFIX + String.format("%02d", i + 1);
      valueMap.put(key, record.get(key));
    }
  }

  /**
   * Creates a partition {@code Key}.
   *
   * @param warehouseId a warehouse ID
   * @param itemId      an item ID
   * @return a {@code Key} object
   */
  public static Key createPartitionKey(int warehouseId, int itemId) {
    ArrayList<Value<?>> keys = new ArrayList<>();
    keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
    keys.add(new IntValue(KEY_ITEM_ID, itemId));
    return new Key(keys);
  }

  /**
   * Creates a {@code Get} object.
   *
   * @param warehouseId a warehouse ID
   * @param itemId      an item ID
   * @return a {@code Get} object
   */
  public static Get createGet(int warehouseId, int itemId) {
    Key partitionKey = createPartitionKey(warehouseId, itemId);
    return new Get(partitionKey).forTable(TABLE_NAME);
  }

  /**
   * Creates a {@code Put} object.
   *
   * @return a {@code Put} object
   */
  @Override
  public Put createPut() {
    Key partitionKey = createPartitionKey();
    ArrayList<Value<?>> values = createValues();
    return new Put(partitionKey).forTable(TABLE_NAME).withValues(values);
  }
}
