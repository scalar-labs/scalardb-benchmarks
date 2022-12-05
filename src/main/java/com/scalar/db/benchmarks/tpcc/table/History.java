package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Put;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.UUID;
import org.apache.commons.csv.CSVRecord;

public class History extends TpccRecord {

  public static final String TABLE_NAME = "history";
  public static final String COLUMN_PREFIX = "h_";
  public static final String KEY_ID = "h_id";
  public static final String KEY_CUSTOMER_ID = "h_c_id";
  public static final String KEY_CUSTOMER_DID = "h_c_d_id";
  public static final String KEY_CUSTOMER_WID = "h_c_w_id";
  public static final String KEY_DISTRICT_ID = "h_d_id";
  public static final String KEY_WAREHOUSE_ID = "h_w_id";
  public static final String KEY_DATE = "h_date";
  public static final String KEY_AMOUNT = "h_amount";
  public static final String KEY_DATA = "h_data";

  public static final int MIN_DATA = 12;
  public static final int MAX_DATA = 24;

  /**
   * Constructs a {@code Customer} with specified parameters.
   *
   * @param customerId a customer ID
   * @param customerDistrictId customer's district ID
   * @param customerWarehouseId customer's warehouse ID
   * @param districtId a district ID
   * @param warehouseId a warehouse ID
   * @param date payment date
   * @param amount payment amount
   * @param data history data
   */
  public History(
      int customerId,
      int customerDistrictId,
      int customerWarehouseId,
      int districtId,
      int warehouseId,
      Date date,
      double amount,
      String data) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_ID, UUID.randomUUID().toString());

    valueMap = new HashMap<>();
    valueMap.put(KEY_CUSTOMER_ID, customerId);
    valueMap.put(KEY_CUSTOMER_DID, customerDistrictId);
    valueMap.put(KEY_CUSTOMER_WID, customerWarehouseId);
    valueMap.put(KEY_DISTRICT_ID, districtId);
    valueMap.put(KEY_WAREHOUSE_ID, warehouseId);
    valueMap.put(KEY_DATE, date);
    valueMap.put(KEY_AMOUNT, amount);
    valueMap.put(KEY_DATA, data);
  }

  /**
   * Constructs a {@code History} with data generation.
   *
   * @param customerId a customer ID
   * @param customerDistrictId customer's district ID
   * @param customerWarehouseId customer's warehouse ID
   * @param districtId a district ID
   * @param warehouseId a warehouse ID
   * @param date payment date
   */
  public History(
      int customerId,
      int customerDistrictId,
      int customerWarehouseId,
      int districtId,
      int warehouseId,
      Date date) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_ID, UUID.randomUUID().toString());

    valueMap = new HashMap<>();
    valueMap.put(KEY_CUSTOMER_ID, customerId);
    valueMap.put(KEY_CUSTOMER_DID, customerDistrictId);
    valueMap.put(KEY_CUSTOMER_WID, customerWarehouseId);
    valueMap.put(KEY_DISTRICT_ID, districtId);
    valueMap.put(KEY_WAREHOUSE_ID, warehouseId);
    valueMap.put(KEY_DATE, date);
    valueMap.put(KEY_AMOUNT, 10.00);
    valueMap.put(KEY_DATA, TpccUtil.randomAlphaString(MIN_DATA, MAX_DATA));
  }

  /**
   * Constructs a {@code History} with a CSV record.
   *
   * @param record a {@code CSVRecord} object
   */
  public History(CSVRecord record) throws ParseException {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_ID, UUID.randomUUID().toString());

    valueMap = new HashMap<>();
    valueMap.put(KEY_CUSTOMER_ID, Integer.parseInt(record.get(KEY_CUSTOMER_ID)));
    valueMap.put(KEY_CUSTOMER_DID, Integer.parseInt(record.get(KEY_CUSTOMER_DID)));
    valueMap.put(KEY_CUSTOMER_WID, Integer.parseInt(record.get(KEY_CUSTOMER_WID)));
    valueMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));
    valueMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    valueMap.put(KEY_DATE, dateFormat.parse(record.get(KEY_DATE)));
    valueMap.put(KEY_AMOUNT, Double.parseDouble(record.get(KEY_AMOUNT)));
    valueMap.put(KEY_DATA, record.get(KEY_DATA));
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
