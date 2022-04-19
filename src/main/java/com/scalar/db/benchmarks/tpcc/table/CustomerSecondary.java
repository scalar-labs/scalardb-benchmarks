package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class CustomerSecondary extends TpccRecordBase {
  public static final String TABLE_NAME = "customer_secondary";
  public static final String COLUMN_PREFIX = "c_";
  public static final String KEY_WAREHOUSE_ID = "c_w_id";
  public static final String KEY_DISTRICT_ID = "c_d_id";
  public static final String KEY_LAST = "c_last";
  public static final String KEY_FIRST = "c_first";
  public static final String KEY_CUSTOMER_ID = "c_id";

  /**
   * Constructs a {@code CustomerSecondary}.
   */
  public CustomerSecondary(int warehouseId, int districtId, String last, String first,
      int customerId) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);
    partitionKeyMap.put(KEY_LAST, last);

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_FIRST, first);
    clusteringKeyMap.put(KEY_CUSTOMER_ID, customerId);
  }

  /**
   * Constructs a {@code CustomerSecondary} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public CustomerSecondary(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));
    partitionKeyMap.put(KEY_LAST, record.get(KEY_LAST));

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_FIRST, record.get(KEY_FIRST));
    clusteringKeyMap.put(KEY_CUSTOMER_ID, Integer.parseInt(record.get(KEY_CUSTOMER_ID)));
  }

  /**
   * Creates a partition {@code Key}.
   */
  public static Key createPartitionKey(int warehouseId, int districtId, String lastName) {
    ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
    keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
    keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
    keys.add(new TextValue(KEY_LAST, lastName));
    return new Key(keys);
  }

  /**
   * Creates a clustering {@code Key}.
   */
  public static Key createClusteringKey(String firstName, int customerId) {
    ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
    keys.add(new TextValue(KEY_FIRST, firstName));
    keys.add(new IntValue(KEY_CUSTOMER_ID, customerId));
    return new Key(keys);
  }

  /**
   * Inserts a {@code CustomerSecondary} record as a transaction.
   * 
   * @param manager a {@code DistributedTransactionManager} object
   */
  public void insert(DistributedTransactionManager manager) throws TransactionException {
    Key partitionKey = createPartitionKey();
    Key clusteringKey = createClusteringKey();
    insert(manager, TABLE_NAME, partitionKey, clusteringKey);
  }
}
