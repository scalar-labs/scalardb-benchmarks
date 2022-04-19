package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class Item extends TpccRecordBase {
  public static final String TABLE_NAME = "item";
  public static final String COLUMN_PREFIX = "i_";
  public static final String KEY_ID = "i_id";
  public static final String KEY_NAME = "i_name";
  public static final String KEY_PRICE = "i_price";
  public static final String KEY_DATA = "i_data";
  public static final String KEY_IM_ID = "i_im_id";

  public static final int ITEMS = 100000;
  public static final int MIN_NAME = 14;
  public static final int MAX_NAME = 24;
  public static final int MIN_DATA = 26;
  public static final int MAX_DATA = 50;
  public static final int UNUSED_ID = 1;

  /**
   * Constructs a {@code Item} with data generation.
   */
  public Item(int itemId) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_ID, itemId);

    valueMap = new HashMap<String,Object>();
    valueMap.put(KEY_NAME, TpccUtil.randomAlphaString(MIN_NAME, MAX_NAME));
    valueMap.put(KEY_PRICE, TpccUtil.randomDouble(100, 1000, 100));
    valueMap.put(KEY_DATA, TpccUtil.getRandomStringWithOriginal(MIN_DATA, MAX_DATA, 10));
    valueMap.put(KEY_IM_ID, TpccUtil.randomInt(1, 10000));
  }

  /**
   * Constructs a {@code Item} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public Item(CSVRecord record) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_ID, Integer.parseInt(record.get(KEY_ID)));

    valueMap = new HashMap<String,Object>();
    valueMap.put(KEY_NAME, record.get(KEY_NAME));
    valueMap.put(KEY_PRICE, Double.parseDouble(record.get(KEY_PRICE)));
    valueMap.put(KEY_DATA, record.get(KEY_DATA));
    valueMap.put(KEY_IM_ID, Integer.parseInt(record.get(KEY_IM_ID)));
  }

  /**
   * Creates a partition {@code Key}.
   */
  public static Key createPartitionKey(int itemId) {
    return new Key(KEY_ID, itemId);
  }

  /**
   * Inserts a {@code Item} record as a transaction.
   * 
   * @param manager a {@code DistributedTransactionManager} object
   */
  public void insert(DistributedTransactionManager manager) throws TransactionException {
    Key key = createPartitionKey();
    ArrayList<Value<?>> values = createValues();
    insert(manager, TABLE_NAME, key, values);
  }
}
