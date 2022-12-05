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

public class District extends TpccRecord {

  public static final String TABLE_NAME = "district";
  public static final String COLUMN_PREFIX = "d_";
  public static final String KEY_WAREHOUSE_ID = "d_w_id";
  public static final String KEY_ID = "d_id";
  public static final String KEY_NAME = "d_name";
  public static final String KEY_ADDRESS = "d_address";
  public static final String KEY_STREET_1 = "d_street_1";
  public static final String KEY_STREET_2 = "d_street_2";
  public static final String KEY_CITY = "d_city";
  public static final String KEY_STATE = "d_state";
  public static final String KEY_ZIP = "d_zip";
  public static final String KEY_TAX = "d_tax";
  public static final String KEY_YTD = "d_ytd";
  public static final String KEY_NEXT_O_ID = "d_next_o_id";

  public static final int CUSTOMERS = 3000;
  public static final int ORDERS = 3000;
  public static final int MIN_NAME = 6;
  public static final int MAX_NAME = 10;

  /**
   * Constructs a {@code District} with ytd.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param ytd a YTD balance
   */
  public District(int warehouseId, int districtId, double ytd) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_ID, districtId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_YTD, ytd);
  }

  /**
   * Constructs a {@code District} with a next order ID.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param nextOrderId a next order ID
   */
  public District(int warehouseId, int districtId, int nextOrderId) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_ID, districtId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_NEXT_O_ID, nextOrderId);
  }

  /**
   * Constructs a {@code District} with data generation.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   */
  public District(int warehouseId, int districtId) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_ID, districtId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_NAME, TpccUtil.randomAlphaString(MIN_NAME, MAX_NAME));
    valueMap.put(KEY_ADDRESS, new Address(COLUMN_PREFIX));
    valueMap.put(KEY_TAX, TpccUtil.randomDouble(0, 2000, 10000));
    valueMap.put(KEY_YTD, 30000.00);
    valueMap.put(KEY_NEXT_O_ID, 3001);
  }

  /**
   * Constructs a {@code District} with a CSV record.
   *
   * @param record a {@code CSVRecord} object
   */
  public District(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_ID, Integer.parseInt(record.get(KEY_ID)));

    valueMap = new HashMap<>();
    valueMap.put(KEY_NAME, record.get(KEY_NAME));
    valueMap.put(
        KEY_ADDRESS,
        new Address(
            COLUMN_PREFIX,
            record.get(KEY_STREET_1),
            record.get(KEY_STREET_2),
            record.get(KEY_CITY),
            record.get(KEY_STATE),
            record.get(KEY_ZIP)));
    valueMap.put(KEY_TAX, Double.parseDouble(record.get(KEY_TAX)));
    valueMap.put(KEY_YTD, Double.parseDouble(record.get(KEY_YTD)));
    valueMap.put(KEY_NEXT_O_ID, Integer.parseInt(record.get(KEY_NEXT_O_ID)));
  }

  /**
   * Creates a partition {@code Key}.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @return a {@code Key} object
   */
  public static Key createPartitionKey(int warehouseId, int districtId) {
    ArrayList<Value<?>> keys = new ArrayList<>();
    keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
    keys.add(new IntValue(KEY_ID, districtId));
    return new Key(keys);
  }

  /**
   * Creates a {@code Get} object.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @return a {@code Get} object
   */
  public static Get createGet(int warehouseId, int districtId) {
    Key partitionKey = createPartitionKey(warehouseId, districtId);
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
