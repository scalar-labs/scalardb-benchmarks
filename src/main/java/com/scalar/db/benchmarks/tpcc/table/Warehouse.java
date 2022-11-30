package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class Warehouse extends TpccRecord {

  public static final String TABLE_NAME = "warehouse";
  public static final String COLUMN_PREFIX = "w_";
  public static final String KEY_ID = "w_id";
  public static final String KEY_NAME = "w_name";
  public static final String KEY_ADDRESS = "w_address";
  public static final String KEY_STREET_1 = "w_street_1";
  public static final String KEY_STREET_2 = "w_street_2";
  public static final String KEY_CITY = "w_city";
  public static final String KEY_STATE = "w_state";
  public static final String KEY_ZIP = "w_zip";
  public static final String KEY_TAX = "w_tax";
  public static final String KEY_YTD = "w_ytd";

  public static final int DISTRICTS = 10;
  public static final int STOCKS = 100000;
  public static final int MIN_NAME = 6;
  public static final int MAX_NAME = 10;

  /**
   * Constructs a {@code Warehouse} with ytd.
   *
   * @param warehouseId a warehouse ID
   * @param ytd         a YTD balance
   */
  public Warehouse(int warehouseId, double ytd) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_ID, warehouseId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_YTD, ytd);
  }

  /**
   * Constructs a {@code Warehouse} with data generation.
   *
   * @param warehouseId a warehouse ID
   */
  public Warehouse(int warehouseId) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_ID, warehouseId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_NAME, TpccUtil.randomAlphaString(MIN_NAME, MAX_NAME));
    valueMap.put(KEY_ADDRESS, new Address(COLUMN_PREFIX));
    valueMap.put(KEY_TAX, TpccUtil.randomDouble(0, 2000, 10000));
    valueMap.put(KEY_YTD, 300000.00);
  }

  /**
   * Constructs a {@code Warehouse} with a CSV record.
   *
   * @param record a {@code CSVRecord} object
   */
  public Warehouse(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_ID, Integer.parseInt(record.get(KEY_ID)));

    valueMap = new HashMap<>();
    valueMap.put(KEY_NAME, record.get(KEY_NAME));
    valueMap.put(KEY_ADDRESS,
        new Address(COLUMN_PREFIX, record.get(KEY_STREET_1), record.get(KEY_STREET_2),
            record.get(KEY_CITY), record.get(KEY_STATE), record.get(KEY_ZIP)));
    valueMap.put(KEY_TAX, Double.parseDouble(record.get(KEY_TAX)));
    valueMap.put(KEY_YTD, Double.parseDouble(record.get(KEY_TAX)));
  }

  /**
   * Creates a partition {@code Key}.
   *
   * @param warehouseId a warehouse ID
   * @return a {@code Key} object
   */
  public static Key createPartitionKey(int warehouseId) {
    return new Key(KEY_ID, warehouseId);
  }

  /**
   * Creates a {@code Get} object.
   *
   * @return a {@code Get} object
   */
  public static Get createGet(int warehouseId) {
    Key partitionKey = createPartitionKey(warehouseId);
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
