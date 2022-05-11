package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class NewOrder extends TpccRecord {
  public static final String TABLE_NAME = "new_order";
  public static final String COLUMN_PREFIX = "no_";

  public static final String KEY_WAREHOUSE_ID = "no_w_id";
  public static final String KEY_DISTRICT_ID = "no_d_id";
  public static final String KEY_ORDER_ID = "no_o_id";

  /**
   * Constructs a {@code NewOrder}.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param orderId an order ID
   */
  public NewOrder(int warehouseId, int districtId, int orderId) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ORDER_ID, orderId);
  }

  /**
   * Constructs a {@code NewOrder} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public NewOrder(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ORDER_ID, Integer.parseInt(record.get(KEY_ORDER_ID)));
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
    return new Key(KEY_ORDER_ID, orderId);
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
   * Creates a {@code Delete} object.
   */
  public static Delete createDelete(int warehouseId, int districtId, int orderId) {
    Key partitionKey = createPartitionKey(warehouseId, districtId);
    Key clusteringKey = createClusteringKey(orderId);
    return new Delete(partitionKey, clusteringKey).forTable(TABLE_NAME);
  }

  /**
   * Creates a {@code Scan} object for the oldest outstanding new-order.
   */
  public static Scan createScan(int warehouseId, int districtId) {
    Key partitionKey = createPartitionKey(warehouseId, districtId);
    return new Scan(partitionKey)
        .forTable(TABLE_NAME)
        .withOrdering(new Ordering(KEY_ORDER_ID, Scan.Ordering.Order.ASC))
        .withLimit(1);
  }
}
