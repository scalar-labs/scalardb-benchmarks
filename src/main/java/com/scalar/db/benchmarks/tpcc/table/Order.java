package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class Order extends TpccRecord {
  public static final String TABLE_NAME = "oorder";
  public static final String COLUMN_PREFIX = "o_";
  public static final String KEY_WAREHOUSE_ID = "o_w_id";
  public static final String KEY_DISTRICT_ID = "o_d_id";
  public static final String KEY_ID = "o_id";
  public static final String KEY_CUSTOMER_ID = "o_c_id";
  public static final String KEY_CARRIER_ID = "o_carrier_id";
  public static final String KEY_OL_CNT = "o_ol_cnt";
  public static final String KEY_ALL_LOCAL = "o_all_local";
  public static final String KEY_ENTRY_D = "o_entry_d";

  /**
   * Constructs a {@code Order} with a carrier ID for update.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param orderId an order ID
   * @param carrierId a carrier ID
   */
  public Order(int warehouseId, int districtId, int orderId, int carrierId) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ID, orderId);

    valueMap = new HashMap<String,Object>();
    valueMap.put(KEY_CARRIER_ID, carrierId);
  }

  /**
   * Constructs a {@code Order} with specified parameters for insert.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param orderId an order ID
   * @param customerId a customer ID
   * @param carrierId a carrier ID
   * @param number number of order lines
   * @param local 1 if the order includes only home order lines, 0 otherwise
   * @param date entry date of this order
   */
  public Order(int warehouseId, int districtId, int orderId, int customerId, int carrierId,
      int number, int local, Date date) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ID, orderId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_CUSTOMER_ID, customerId);
    valueMap.put(KEY_CARRIER_ID, carrierId);
    valueMap.put(KEY_OL_CNT, number);
    valueMap.put(KEY_ALL_LOCAL, local);
    valueMap.put(KEY_ENTRY_D, date);
  }

  /**
   * Constructs a {@code Order} with data generation.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param orderId an order ID
   * @param customerId a customer ID
   * @param date entry date of this order
   */
  public Order(int warehouseId, int districtId, int orderId, int customerId, Date date) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ID, orderId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_CUSTOMER_ID, customerId);
    if (orderId < 2101) {
      valueMap.put(KEY_CARRIER_ID, TpccUtil.randomInt(1, 10));
    } else {
      valueMap.put(KEY_CARRIER_ID, 0);
    }
    valueMap.put(KEY_OL_CNT,
        TpccUtil.randomInt(OrderLine.MIN_PER_ORDER, OrderLine.MAX_PER_ORDER));
    valueMap.put(KEY_ALL_LOCAL, 1);
    valueMap.put(KEY_ENTRY_D, date);
  }

  /**
   * Constructs a {@code Order} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public Order(CSVRecord record) throws ParseException {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ID, Integer.parseInt(record.get(KEY_ID)));

    valueMap = new HashMap<>();
    valueMap.put(KEY_CUSTOMER_ID, Integer.parseInt(record.get(KEY_CUSTOMER_ID)));
    if (!record.get(KEY_CARRIER_ID).isEmpty()
        && !record.get(KEY_CARRIER_ID).equals("\\N")) {
      valueMap.put(KEY_CARRIER_ID, Integer.parseInt(record.get(KEY_CARRIER_ID)));
    } else {
      valueMap.put(KEY_CARRIER_ID, 0);
    }
    valueMap.put(KEY_OL_CNT, Integer.parseInt(record.get(KEY_OL_CNT)));
    valueMap.put(KEY_ALL_LOCAL, Integer.parseInt(record.get(KEY_ALL_LOCAL)));
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    valueMap.put(KEY_ENTRY_D, dateFormat.parse(record.get(KEY_ENTRY_D)));
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
    keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
    return new Key(keys);
  }

  /**
   * Creates a clustering {@code Key}.
   * 
   * @param orderId an order ID
   * @return a {@code Key} object
   */
  public static Key createClusteringKey(int orderId) {
    return new Key(KEY_ID, orderId);
  }

  /**
   * Creates a {@code Get} object.
   */
  public static Get createGet(int warehouseId, int districtId, int orderId) {
    Key parttionkey = createPartitionKey(warehouseId, districtId);
    Key clusteringKey = createClusteringKey(orderId);
    return new Get(parttionkey, clusteringKey).forTable(TABLE_NAME);
  }

  /**
   * Creates a {@code Put} object.
   *
   * @return a {@code Put} object
   */
  @Override
  public Put createPut() {
    Key partitionkey = createPartitionKey();
    Key clusteringKey = createClusteringKey();
    ArrayList<Value<?>> values = createValues();
    return new Put(partitionkey, clusteringKey).forTable(TABLE_NAME).withValues(values);
  }

  public int getOrderLineCount() {
    return (Integer)valueMap.get(KEY_OL_CNT);
  }
}
