package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Put;
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
   */
  public NewOrder(int warehouseId, int districtId, int orderId) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ORDER_ID, orderId);
  }

  /**
   * Constructs a {@code NewOrder} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public NewOrder(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ORDER_ID, Integer.parseInt(record.get(KEY_ORDER_ID)));
  }

  /**
   * Creates a partition {@code Key}.
   */
  public static Key createPartitionKey(int warehouseId, int districtId) {
    ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
    keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
    keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
    return new Key(keys);
  }

  /**
   * Creates a clustering {@code Key}.
   */
  public static Key createClusteringKey(int orderId) {
    return new Key(KEY_ORDER_ID, orderId);
  }

  /**
   * Creates a {@code Put} object.
   */
  public Put createPut() {
    Key parttionkey = createPartitionKey();
    Key clusteringKey = createClusteringKey();
    return new Put(parttionkey, clusteringKey).forTable(TABLE_NAME);
  }
}
