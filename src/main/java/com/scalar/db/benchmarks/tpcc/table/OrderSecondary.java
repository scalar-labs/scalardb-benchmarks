package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
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
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param customerId a customer ID
   * @param orderId an order ID
   */
  public OrderSecondary(int warehouseId, int districtId, int customerId, int orderId) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);
    partitionKeyMap.put(KEY_CUSTOMER_ID, customerId);

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ORDER_ID, orderId);
  }

  /**
   * Constructs a {@code OrderSecondary} with a CSV record.
   *
   * @param record a {@code CSVRecord} object
   */
  public OrderSecondary(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));
    partitionKeyMap.put(KEY_CUSTOMER_ID, Integer.parseInt(record.get(KEY_CUSTOMER_ID)));

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ORDER_ID, Integer.parseInt(record.get(KEY_ORDER_ID)));
  }

  /**
   * Creates a partition {@code Key}.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param customerId a customer ID
   * @return a {@code Key} object
   */
  public static Key createPartitionKey(int warehouseId, int districtId, int customerId) {
    ArrayList<Value<?>> keys = new ArrayList<>();
    keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
    keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
    keys.add(new IntValue(KEY_CUSTOMER_ID, customerId));
    return new Key(keys);
  }

  /**
   * Creates a clustering {@code Key}.
   *
   * @param orderId an order ID
   * @return a {@code Key} object
   */
  public static Key createClusteringKey(int orderId) {
    ArrayList<Value<?>> keys = new ArrayList<>();
    keys.add(new IntValue(KEY_ORDER_ID, orderId));
    return new Key(keys);
  }

  /**
   * Creates a {@code Put} object.
   *
   * @return a {@code Put} object
   */
  @Override
  public Put createPut() {
    Key partitionKey = createPartitionKey();
    Key clusteringKey = createClusteringKey();
    return new Put(partitionKey, clusteringKey).forTable(TABLE_NAME);
  }

  /**
   * Creates a {@code Scan} object for the last order of a customer.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param customerId a customer ID
   * @return a {@code Scan} object for the last order of a customer
   */
  public static Scan createScan(int warehouseId, int districtId, int customerId) {
    Key partitionKey = createPartitionKey(warehouseId, districtId, customerId);
    return new Scan(partitionKey)
        .forTable(TABLE_NAME)
        .withOrdering(new Ordering(KEY_ORDER_ID, Scan.Ordering.Order.DESC))
        .withLimit(1);
  }
}
