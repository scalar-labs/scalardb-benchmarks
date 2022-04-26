package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
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

public class OrderLine extends TpccRecord {
  public static final String TABLE_NAME = "order_line";
  public static final String COLUMN_PREFIX = "ol_";
  public static final String KEY_WAREHOUSE_ID = "ol_w_id";
  public static final String KEY_DISTRICT_ID = "ol_d_id";
  public static final String KEY_ORDER_ID = "ol_o_id";
  public static final String KEY_NUMBER = "ol_number";
  public static final String KEY_ITEM_ID = "ol_i_id";
  public static final String KEY_DELIVERY_D = "ol_delivery_d";
  public static final String KEY_AMOUNT = "ol_amount";
  public static final String KEY_SUPPLY_W_ID = "ol_supply_w_id";
  public static final String KEY_QUANTITY = "ol_quantity";
  public static final String KEY_DIST_INFO = "ol_dist_info";

  public static final int MIN_PER_ORDER = 5;
  public static final int MAX_PER_ORDER = 15;
  public static final int DIST_INFO_SIZE = 24;

  /**
   * Constructs a {@code OrderLine} with delivery date for update.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param orderId an order ID
   * @param number an order-line number
   * @param deliveryDate district information
   */
  public OrderLine(int warehouseId, int districtId, int orderId, int number, Date deliveryDate) {
    partitionKeyMap = new LinkedHashMap<String,Object>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<String,Object>();
    clusteringKeyMap.put(KEY_ORDER_ID, orderId);
    clusteringKeyMap.put(KEY_NUMBER, number);

    valueMap = new HashMap<String,Object>();
    valueMap.put(KEY_DELIVERY_D, deliveryDate);
  }

  /**
   * Constructs a {@code OrderLine} with specified parameters for insert.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param orderId an order ID
   * @param number an order-line number
   * @param supplyWarehouseId a supplier warehouse ID in this order line
   * @param amount amount for the item in this order line
   * @param quantity quantity of the item in this order line
   * @param itemId an item ID in this order line
   * @param info district information
   */
   public OrderLine(int warehouseId, int districtId, int orderId, int number,
      int supplyWarehouseId, double amount, int quantity, int itemId, String info) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ORDER_ID, orderId);
    clusteringKeyMap.put(KEY_NUMBER, number);

    valueMap = new HashMap<>();
    valueMap.put(KEY_ITEM_ID, itemId);
    valueMap.put(KEY_SUPPLY_W_ID, supplyWarehouseId);
    valueMap.put(KEY_DELIVERY_D, null);
    valueMap.put(KEY_AMOUNT, amount);
    valueMap.put(KEY_QUANTITY, quantity);
    valueMap.put(KEY_DIST_INFO, info);
  }

  /**
   * Constructs a {@code OrderLine} with data generation.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param orderId an order ID
   * @param number an order-line number
   * @param supplyWarehouseId a supplier warehouse ID in this order line
   * @param itemId an item ID in this order line
   * @param date delivery date of this order
   */
  public OrderLine(int warehouseId, int districtId, int orderId, int number,
      int supplyWarehouseId, int itemId, Date date) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ORDER_ID, orderId);
    clusteringKeyMap.put(KEY_NUMBER, number);

    valueMap = new HashMap<>();
    valueMap.put(KEY_ITEM_ID, itemId);
    valueMap.put(KEY_SUPPLY_W_ID, supplyWarehouseId);
    if (orderId < 2101) {
      valueMap.put(KEY_DELIVERY_D, date);
      valueMap.put(KEY_AMOUNT, 0.00);
    } else {
      valueMap.put(KEY_DELIVERY_D, null);
      valueMap.put(KEY_AMOUNT, TpccUtil.randomDouble(1, 999999, 100));
    }
    valueMap.put(KEY_QUANTITY, 5);
    valueMap.put(KEY_DIST_INFO, TpccUtil.randomAlphaString(DIST_INFO_SIZE));
  }

  /**
   * Constructs a {@code OrderLine} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public OrderLine(CSVRecord record) throws ParseException {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));

    clusteringKeyMap = new LinkedHashMap<>();
    clusteringKeyMap.put(KEY_ORDER_ID, Integer.parseInt(record.get(KEY_ORDER_ID)));
    clusteringKeyMap.put(KEY_NUMBER, Integer.parseInt(record.get(KEY_NUMBER)));

    valueMap = new HashMap<>();
    valueMap.put(KEY_ITEM_ID, Integer.parseInt(record.get(KEY_ITEM_ID)));
    valueMap.put(KEY_SUPPLY_W_ID, Integer.parseInt(record.get(KEY_SUPPLY_W_ID)));
    valueMap.put(KEY_AMOUNT, Double.parseDouble(record.get(KEY_AMOUNT)));
    valueMap.put(KEY_QUANTITY, Integer.parseInt(record.get(KEY_QUANTITY)));
    valueMap.put(KEY_DIST_INFO, record.get(KEY_DIST_INFO));
    if (!record.get(KEY_DELIVERY_D).isEmpty()
        && !record.get(KEY_DELIVERY_D).equals("\\N")) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      valueMap.put(KEY_DELIVERY_D, dateFormat.parse(record.get(KEY_DELIVERY_D)));
    } else {
      valueMap.put(KEY_DELIVERY_D, null);
    }
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
   */
  public static Key createClusteringKey(int orderId, int orderLineNumber) {
    ArrayList<Value<?>> keys = new ArrayList<>();
    keys.add(new IntValue(KEY_ORDER_ID, orderId));
    keys.add(new IntValue(KEY_NUMBER, orderLineNumber));
    return new Key(keys);
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

  /**
   * Creates a {@code Scan} object for order-lines with a specified order ID.
   */
  public static Scan createScan(int warehouseId, int districtId, int orderId) {
    return createScan(warehouseId, districtId, orderId, orderId);
  }

  /**
   * Creates a {@code Scan} object for order-lines with a range of order IDs.
   */
  public static Scan createScan(int warehouseId, int districtId, int orderIdStart, int orderIdEnd) {
    Key parttionkey = createPartitionKey(warehouseId, districtId);
    Key start = new Key(OrderLine.KEY_ORDER_ID, orderIdStart);
    Key end = new Key(OrderLine.KEY_ORDER_ID, orderIdEnd);
    return new Scan(parttionkey).forTable(TABLE_NAME).withStart(start).withEnd(end);
  }
}
