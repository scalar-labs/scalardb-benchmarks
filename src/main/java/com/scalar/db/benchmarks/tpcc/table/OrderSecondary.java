package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class OrderSecondary extends TpccRecord {
  public static final String TABLE_NAME = "order_secondary";
  public static final String KEY_WAREHOUSE_ID = "o_w_id";
  public static final String KEY_DISTRICT_ID = "o_d_id";
  public static final String KEY_CUSTOMER_ID = "o_c_id";
  public static final String KEY_ORDER_ID = "o_id";

  /**
   * Constructs a {@code OrderSecondary}.
   */
  public OrderSecondary(int warehouseId, int districtId, int customerId, int orderId) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);
    partitionKeyMap.put(KEY_CUSTOMER_ID, customerId);

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ORDER_ID, orderId);
  }

  /**
   * Constructs a {@code OrderSecondary} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public OrderSecondary(CSVRecord record) throws ParseException {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));
    partitionKeyMap.put(KEY_CUSTOMER_ID, Integer.parseInt(record.get(KEY_CUSTOMER_ID)));

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ORDER_ID, Integer.parseInt(record.get(KEY_ORDER_ID)));
  }

  /**
   * Creates a partition {@code Key}.
   */
  public static Key createPartitionKey(int warehouseId, int districtId, int customerId) {
    ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
    keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
    keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
    keys.add(new IntValue(KEY_CUSTOMER_ID, customerId));
    return new Key(keys);
  }

  /**
   * Creates a clustering {@code Key}.
   */
  public static Key createClusteringKey(int orderId) {
    ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
    keys.add(new IntValue(KEY_ORDER_ID, orderId));
    return new Key(keys);
  }

  /**
   * Creates a {@code Put} object.
   */
  public Put createPut() {
    Key parttionkey = createPartitionKey();
    Key clusteringKey = createClusteringKey();
    return new Put(parttionkey, clusteringKey).forTable(TABLE_NAME);
  }

  /**
   * Creates a {@code Scan} object for the last order of a customer.
   */
  public static Scan createScan(int warehouseId, int districtId, int customerId) {
    Key parttionkey = createPartitionKey(warehouseId, districtId, customerId);
    return new Scan(parttionkey)
        .forTable(TABLE_NAME)
        .withOrdering(new Ordering(KEY_ORDER_ID, Scan.Ordering.Order.DESC))
        .withLimit(1);
  }
}
