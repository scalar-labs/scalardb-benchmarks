package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.exception.transaction.TransactionException;
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

public class Order extends TpccRecordBase {
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
   * Constructs a {@code Order} with specified parameters.
   */
  public Order(int warehouseId, int districtId, int orderId, int customerId, int carrierId,
      int number, int local, Date date) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ID, orderId);

    valueMap = new HashMap<String,Object>();
    valueMap.put(KEY_CUSTOMER_ID, customerId);
    valueMap.put(KEY_CARRIER_ID, carrierId);
    valueMap.put(KEY_OL_CNT, number);
    valueMap.put(KEY_ALL_LOCAL, local);
    valueMap.put(KEY_ENTRY_D, date);
  }

  /**
   * Constructs a {@code Order} with data generation.
   */
  public Order(int warehouseId, int districtId, int orderId, int customerId, Date date) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ID, orderId);

    valueMap = new HashMap<String,Object>();
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
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ID, Integer.parseInt(record.get(KEY_ID)));

    valueMap = new HashMap<String,Object>();
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
    try {
      valueMap.put(KEY_ENTRY_D, dateFormat.parse(record.get(KEY_ENTRY_D)));
    } catch (ParseException e) {
      throw e;
    }
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
    return new Key(KEY_ID, orderId);
  }

  /**
   * Inserts a {@code Order} record as a transaction.
   * 
   * @param manager a {@code DistributedTransactionManager} object
   */
  public void insert(DistributedTransactionManager manager) throws TransactionException {
    Key partitionKey = createPartitionKey();
    Key clusteringKey = createClusteringKey();
    ArrayList<Value<?>> values = createValues();
    insert(manager, TABLE_NAME, partitionKey, clusteringKey, values);
  }

  public int getOrderLineCount() {
    return (Integer)valueMap.get(KEY_OL_CNT);
  }
}
