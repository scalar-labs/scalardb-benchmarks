package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.commons.csv.CSVRecord;

public class Customer extends TpccRecord {
  public static final String TABLE_NAME = "customer";
  public static final String COLUMN_PREFIX = "c_";
  public static final String KEY_WAREHOUSE_ID = "c_w_id";
  public static final String KEY_DISTRICT_ID = "c_d_id";
  public static final String KEY_ID = "c_id";
  public static final String KEY_FIRST = "c_first";
  public static final String KEY_MIDDLE = "c_middle";
  public static final String KEY_LAST = "c_last";
  public static final String KEY_DISCOUNT = "c_discount";
  public static final String KEY_CREDIT = "c_credit";
  public static final String KEY_CREDIT_LIM = "c_credit_lim";
  public static final String KEY_BALANCE = "c_balance";
  public static final String KEY_YTD_PAYMENT = "c_ytd_payment";
  public static final String KEY_PAYMENT_CNT = "c_payment_cnt";
  public static final String KEY_DELIVERY_CNT = "c_delivery_cnt";
  public static final String KEY_ADDRESS = "c_address";
  public static final String KEY_STREET_1 = "c_street_1";
  public static final String KEY_STREET_2 = "c_street_2";
  public static final String KEY_CITY = "c_city";
  public static final String KEY_STATE = "c_state";
  public static final String KEY_ZIP = "c_zip";
  public static final String KEY_PHONE = "c_phone";
  public static final String KEY_SINCE = "c_since";
  public static final String KEY_DATA = "c_data";
  public static final String KEY_INDEX = "c_index";

  public static final int UNUSED_ID = 0;
  public static final int MIN_FIRST = 8;
  public static final int MAX_FIRST = 16;
  public static final int MIN_DATA = 300;
  public static final int MAX_DATA = 500;
  public static final int PHONE_SIZE = 16;

  public static final Comparator<Result> FIRST_NAME_COMPARATOR = (a, b) -> {
    String firstNameA = a.getValue(KEY_FIRST).get().getAsString().get();
    String firstNameB = b.getValue(KEY_FIRST).get().getAsString().get();
    return firstNameA.compareTo(firstNameB);
  };

  /**
   * Constructs a {@code Customer} for payment transaction.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param customerId a customer ID
   * @param balance a balance of the customer
   * @param ytdPayment a YTD payment amount
   * @param paymentCount number of payments
   * @param data customer data
   */
  public Customer(int warehouseId, int districtId, int customerId,
      double balance, double ytdPayment, int paymentCount, String data) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);
    partitionKeyMap.put(KEY_ID, customerId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_BALANCE, balance);
    valueMap.put(KEY_YTD_PAYMENT, ytdPayment);
    valueMap.put(KEY_PAYMENT_CNT, paymentCount);
    valueMap.put(KEY_DATA, data);
  }

  /**
   * Constructs a {@code Customer} for delivery transaction.
   */
  public Customer(int warehouseId, int districtId, int customerId,
      double balance, int deliveryCount) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);
    partitionKeyMap.put(KEY_ID, customerId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_BALANCE, balance);
    valueMap.put(KEY_DELIVERY_CNT, deliveryCount);
  }

  /**
   * Constructs a {@code Customer} with data generation.
   *
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param customerId a customer ID
   */
  public Customer(int warehouseId, int districtId, int customerId, Date date) {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, warehouseId);
    partitionKeyMap.put(KEY_DISTRICT_ID, districtId);
    partitionKeyMap.put(KEY_ID, customerId);

    valueMap = new HashMap<>();
    valueMap.put(KEY_FIRST, TpccUtil.randomAlphaString(MIN_FIRST, MAX_FIRST));
    valueMap.put(KEY_MIDDLE, "OE");
    if (customerId <= 1000) {
      valueMap.put(KEY_LAST, TpccUtil.getLastName(customerId - 1));
    } else {
      valueMap.put(KEY_LAST, TpccUtil.getNonUniformRandomLastNameForLoad());
    }
    valueMap.put(KEY_DISCOUNT, TpccUtil.randomDouble(0, 5000, 10000));
    if (TpccUtil.randomInt(0, 99) < 10) {
      valueMap.put(KEY_CREDIT, "BC");
    } else {
      valueMap.put(KEY_CREDIT, "GC");
    }
    valueMap.put(KEY_CREDIT_LIM, 50000.00);
    valueMap.put(KEY_BALANCE, 10.00);
    valueMap.put(KEY_YTD_PAYMENT, 10.00);
    valueMap.put(KEY_PAYMENT_CNT, 1);
    valueMap.put(KEY_DELIVERY_CNT, 0);
    valueMap.put(KEY_ADDRESS, new Address(COLUMN_PREFIX));
    valueMap.put(KEY_PHONE, TpccUtil.randomNumberString(PHONE_SIZE));
    valueMap.put(KEY_SINCE, date);
    valueMap.put(KEY_DATA, TpccUtil.randomAlphaString(MIN_DATA, MAX_DATA));
  }

  /**
   * Constructs a {@code Customer} with a CSV record.
   * 
   * @param record a {@code CSVRecord} object
   */
  public Customer(CSVRecord record) throws ParseException {
    partitionKeyMap = new LinkedHashMap<>();
    partitionKeyMap.put(KEY_WAREHOUSE_ID, Integer.parseInt(record.get(KEY_WAREHOUSE_ID)));
    partitionKeyMap.put(KEY_DISTRICT_ID, Integer.parseInt(record.get(KEY_DISTRICT_ID)));
    partitionKeyMap.put(KEY_ID, Integer.parseInt(record.get(KEY_ID)));

    valueMap = new HashMap<>();
    valueMap.put(KEY_FIRST, record.get(KEY_FIRST));
    valueMap.put(KEY_MIDDLE, record.get(KEY_MIDDLE));
    valueMap.put(KEY_LAST, record.get(KEY_LAST));
    valueMap.put(KEY_DISCOUNT, Double.parseDouble(record.get(KEY_DISCOUNT)));
    valueMap.put(KEY_CREDIT, record.get(KEY_CREDIT));
    valueMap.put(KEY_CREDIT_LIM, Double.parseDouble(record.get(KEY_CREDIT_LIM)));
    valueMap.put(KEY_BALANCE, Double.parseDouble(record.get(KEY_BALANCE)));
    valueMap.put(KEY_YTD_PAYMENT, Double.parseDouble(record.get(KEY_YTD_PAYMENT)));
    valueMap.put(KEY_PAYMENT_CNT, Integer.parseInt(record.get(KEY_PAYMENT_CNT)));
    valueMap.put(KEY_DELIVERY_CNT, Integer.parseInt(record.get(KEY_DELIVERY_CNT)));
    valueMap.put(KEY_ADDRESS,
        new Address(COLUMN_PREFIX, record.get(KEY_STREET_1), record.get(KEY_STREET_2),
        record.get(KEY_CITY), record.get(KEY_STATE), record.get(KEY_ZIP)));
    valueMap.put(KEY_PHONE, record.get(KEY_PHONE));
    valueMap.put(KEY_DATA, record.get(KEY_DATA));
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    valueMap.put(KEY_SINCE, dateFormat.parse(record.get(KEY_SINCE)));
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
    keys.add(new IntValue(KEY_ID, customerId));
    return new Key(keys);
  }

  private static String createIndexString(int warehouseId, int districtId, String lastName) {
    return String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + lastName;
  }

  /**
   * Creates a {@code Scan} object.
   *
   * @return a {@code Scan} object
   */
  public static Scan createScan(int warehouseId, int districtId, String lastName) {
    Key key = new Key(KEY_INDEX, createIndexString(warehouseId, districtId, lastName));
    return new Scan(key).forTable(TABLE_NAME);
  }

  /**
   * Creates a {@code Get} object.
   * 
   * @param warehouseId a warehouse ID
   * @param districtId a district ID
   * @param customerId a customer ID
   * @return a {@code Get} object
   */
  public static Get createGet(int warehouseId, int districtId, int customerId) {
    Key partitionKey = createPartitionKey(warehouseId, districtId, customerId);
    return new Get(partitionKey).forTable(TABLE_NAME);
  }

  /**
   * Creates a {@code Put} object.
   *
   * @return a {@code Put} object
   */
  @Override
  public Put createPut() {
    Key partitionKey = createPartitionKey();
    ArrayList<Value<?>> values = createValues();
    return new Put(partitionKey).forTable(TABLE_NAME).withValues(values);
  }

  /**
   * Builds a column for secondary index.
   */
  public void buildIndexColumn() {
    int warehouseId = (int)partitionKeyMap.get(KEY_WAREHOUSE_ID);
    int districtId = (int)partitionKeyMap.get(KEY_DISTRICT_ID);
    String lastName = (String)valueMap.get(KEY_LAST);
    String index = createIndexString(warehouseId, districtId, lastName);
    valueMap.put(KEY_INDEX, index);
  }

  public String getFirstName() {
    return (String)valueMap.get(KEY_FIRST);
  }

  public String getLastName() {
    return (String)valueMap.get(KEY_LAST);
  }
}
