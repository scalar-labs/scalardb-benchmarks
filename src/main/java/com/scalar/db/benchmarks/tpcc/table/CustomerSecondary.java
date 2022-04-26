package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class CustomerSecondary extends TpccRecord {
  public static final String TABLE_NAME = "customer_secondary";
  public static final String COLUMN_PREFIX = "c_";
  public static final String KEY_WAREHOUSE_ID = "c_w_id";
  public static final String KEY_DISTRICT_ID = "c_d_id";
  public static final String KEY_LAST = "c_last";
  public static final String KEY_FIRST = "c_first";
  public static final String KEY_CUSTOMER_ID = "c_id";

  /**
   * Constructs a {@code CustomerSecondary}.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param last last name of the customer
   * @param first first name of the customer
   * @param customerId a customer ID
   */
  public CustomerSecondary(int warehouseId, int districtId, String last, String first,
      int customerId) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);
    partitionKeyMap.put(KEY_LAST, last);

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_FIRST, first);
    clusteringKeyMap.put(KEY_CUSTOMER_ID, customerId);
  }

  /**
   * Constructs a {@code CustomerSecondary} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public CustomerSecondary(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));
    partitionKeyMap.put(KEY_LAST, record.get(KEY_LAST));

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_FIRST, record.get(KEY_FIRST));
    clusteringKeyMap.put(KEY_CUSTOMER_ID, Integer.parseInt(record.get(KEY_CUSTOMER_ID)));
  }

  /**
   * Creates a partition {@code Key}.
   * 
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param lastName a {@code String} of last name
   * @return a {@code Key} object
   */
  public static Key createPartitionKey(int warehouseId, int districtId, String lastName) {
    ArrayList<Value<?>> keys = new ArrayList<>();
    keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
    keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
    keys.add(new TextValue(KEY_LAST, lastName));
    return new Key(keys);
  }

  /**
   * Creates a clustering {@code Key}.
   * 
   * @param firstName a {@code String} of first name
   * @param customerId a customer ID
   * @return a {@code Key} object
   */
  public static Key createClusteringKey(String firstName, int customerId) {
    ArrayList<Value<?>> keys = new ArrayList<>();
    keys.add(new TextValue(KEY_FIRST, firstName));
    keys.add(new IntValue(KEY_CUSTOMER_ID, customerId));
    return new Key(keys);
  }

  /**
   * Creates a {@code Put} object.
   *
   * @return a {@code Put} object
   */
  public Put createPut() {
    Key partitionkey = createPartitionKey();
    Key clusteringKey = createClusteringKey();
    return new Put(partitionkey, clusteringKey).forTable(TABLE_NAME);
  }

  /**
   * Creates a {@code Scan} object.
   *
   * @return a {@code Scan} object
   */
  public static Scan createScan(int warehouseId, int districtId, String lastName) {
    Key partitionkey = createPartitionKey(warehouseId, districtId, lastName);
    return new Scan(partitionkey).forTable(TABLE_NAME);
  }
}
