package com.scalar.db.benchmarks.tpcc;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import org.apache.commons.csv.CSVRecord;

public class TPCCTable {
  public static final String NAMESPACE = "tpcc";

  public enum Code {
    CUSTOMER,
    CUSTOMER_SECONDARY,
    DISTRICT,
    HISTORY,
    ITEM,
    NEW_ORDER,
    ORDER,
    ORDER_SECONDARY,
    ORDER_LINE,
    STOCK,
    WAREHOUSE
  }

  public interface TPCCRecord {
    public void insert(DistributedTransactionManager manager) throws TransactionException;
  }

  public static abstract class TPCCRecordBase implements TPCCRecord {
    public void insert(DistributedTransactionManager manager, String table, Key partitionKey,
        ArrayList<Value<?>> values) throws TransactionException {
      DistributedTransaction tx = manager.start();
      tx.with(NAMESPACE, table);
      try {
        tx.put(new Put(partitionKey).withValues(values));
        tx.commit();
      } catch (Exception e) {
        tx.abort();
        throw e;
      }
    }

    public void insert(DistributedTransactionManager manager, String table, Key partitionKey,
        Key clusteringKey, ArrayList<Value<?>> values) throws TransactionException {
      DistributedTransaction tx = manager.start();
      tx.with(NAMESPACE, table);
      try {
        tx.put(new Put(partitionKey, clusteringKey).withValues(values));
        tx.commit();
      } catch (Exception e) {
        tx.abort();
        throw e;
      }
    }

    public void insert(DistributedTransactionManager manager, String table, Key partitionKey,
        Key clusteringKey) throws TransactionException {
      DistributedTransaction tx = manager.start();
      tx.with(NAMESPACE, table);
      try {
        tx.put(new Put(partitionKey, clusteringKey));
        tx.commit();
      } catch (Exception e) {
        tx.abort();
        throw e;
      }
    }
  }

  public class Address {
    public static final int MIN_STREET = 10;
    public static final int MAX_STREET = 20;
    public static final int MIN_CITY = 10;
    public static final int MAX_CITY = 20;
    public static final int STATE_SIZE = 2;
    public static final int ZIP_SIZE = 4;
  }

  public static class Customer extends TPCCRecordBase {
    public static final String TABLE_NAME = "customer";
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
    public static final String KEY_STREET_1 = "c_street_1";
    public static final String KEY_STREET_2 = "c_street_2";
    public static final String KEY_CITY = "c_city";
    public static final String KEY_STATE = "c_state";
    public static final String KEY_ZIP = "c_zip";
    public static final String KEY_PHONE = "c_phone";
    public static final String KEY_SINCE = "c_since";
    public static final String KEY_DATA = "c_data";

    public static final int MIN_FIRST = 8;
    public static final int MAX_FIRST = 16;
    public static final int MIN_DATA = 300;
    public static final int MAX_DATA = 500;
    public static final int PHONE_SIZE = 16;

    private int c_w_id;
    private int c_d_id;
    private int c_id;
    private String c_first;
    private String c_middle;
    private String c_last;
    private double c_discount;
    private String c_credit;
    private double c_credit_lim;
    private double c_balance;
    private double c_ytd_payment;
    private int c_payment_cnt;
    private int c_delivery_cnt;
    private String c_street_1;
    private String c_street_2;
    private String c_city;
    private String c_state;
    private String c_zip;
    private String c_phone;
    private Date c_since;
    private String c_data;

    public Customer(int warehouseId, int districtId, int customerId, Date date) {
      c_w_id = warehouseId;
      c_d_id = districtId;
      c_id = customerId;
      c_first = TPCCUtil.randomAlphaString(MIN_FIRST, MAX_FIRST);
      c_middle = "OE";
      if (c_id <= 1000) {
        c_last = TPCCUtil.getLastName(c_id - 1);
      } else {
        c_last = TPCCUtil.getNonUniformRandomLastNameForLoad();
      }
      c_discount = TPCCUtil.randomDouble(0, 5000, 10000);
      if (TPCCUtil.randomInt(0, 99) < 10) {
        c_credit = "BC";
      } else {
        c_credit = "GC";
      }
      c_credit_lim = 50000;
      c_balance = -10.00;
      c_ytd_payment = 10.00;
      c_payment_cnt = 1;
      c_delivery_cnt = 0;
      c_street_1 = TPCCUtil.randomAlphaString(Address.MIN_STREET, Address.MAX_STREET);
      c_street_2 = TPCCUtil.randomAlphaString(Address.MIN_STREET, Address.MAX_STREET);
      c_city = TPCCUtil.randomAlphaString(Address.MIN_CITY, Address.MAX_CITY);
      c_state = TPCCUtil.randomAlphaString(Address.STATE_SIZE);
      c_zip = TPCCUtil.randomNumberString(Address.ZIP_SIZE) + "11111";
      c_phone = TPCCUtil.randomNumberString(PHONE_SIZE);
      c_since = date;
      c_data = TPCCUtil.randomNumberString(MIN_DATA, MAX_DATA);
    }

    public Customer(CSVRecord record) throws ParseException {
      c_w_id = Integer.parseInt(record.get(Customer.KEY_WAREHOUSE_ID));
      c_d_id = Integer.parseInt(record.get(Customer.KEY_DISTRICT_ID));
      c_id = Integer.parseInt(record.get(Customer.KEY_ID));
      c_first = record.get(Customer.KEY_FIRST);
      c_middle = record.get(Customer.KEY_MIDDLE);
      c_last = record.get(Customer.KEY_LAST);
      c_discount = Double.parseDouble(record.get(Customer.KEY_DISCOUNT));
      c_credit = record.get(Customer.KEY_CREDIT);
      c_credit_lim = Double.parseDouble(record.get(Customer.KEY_CREDIT_LIM));
      c_balance = Double.parseDouble(record.get(Customer.KEY_BALANCE));
      c_ytd_payment = Double.parseDouble(record.get(Customer.KEY_YTD_PAYMENT));
      c_payment_cnt = Integer.parseInt(record.get(Customer.KEY_PAYMENT_CNT));
      c_delivery_cnt = Integer.parseInt(record.get(Customer.KEY_DELIVERY_CNT));
      c_street_1 = record.get(Customer.KEY_STREET_1);
      c_street_2 = record.get(Customer.KEY_STREET_2);
      c_city = record.get(Customer.KEY_CITY);
      c_state = record.get(Customer.KEY_STATE);
      c_zip = record.get(Customer.KEY_ZIP);
      c_phone = record.get(Customer.KEY_PHONE);
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      try {
        c_since = dateFormat.parse(record.get(Customer.KEY_SINCE));
      } catch (ParseException e) {
        e.printStackTrace();
        throw e;
      }
      c_data = record.get(Customer.KEY_DATA);
    }

    public static Key createPartitionKey(int warehouseId, int districtId, int customerId) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
      keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
      keys.add(new IntValue(KEY_ID, customerId));
      return new Key(keys);
    }

    public Key createPartitionKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, c_w_id));
      keys.add(new IntValue(KEY_DISTRICT_ID, c_d_id));
      keys.add(new IntValue(KEY_ID, c_id));
      return new Key(keys);
    }

    public ArrayList<Value<?>> createValues() {
      ArrayList<Value<?>> values = new ArrayList<Value<?>>();
      values.add(new TextValue(Customer.KEY_FIRST, c_first));
      values.add(new TextValue(Customer.KEY_MIDDLE, c_middle));
      values.add(new TextValue(Customer.KEY_LAST, c_last));
      values.add(new TextValue(Customer.KEY_STREET_1, c_street_1));
      values.add(new TextValue(Customer.KEY_STREET_2, c_street_2));
      values.add(new TextValue(Customer.KEY_CITY, c_city));
      values.add(new TextValue(Customer.KEY_STATE, c_state));
      values.add(new TextValue(Customer.KEY_ZIP, c_zip));
      values.add(new TextValue(Customer.KEY_PHONE, c_phone));
      values.add(new TextValue(Customer.KEY_CREDIT, c_credit));
      values.add(new TextValue(Customer.KEY_DATA, c_data));
      values.add(new DoubleValue(Customer.KEY_CREDIT_LIM, c_credit_lim));
      values.add(new DoubleValue(Customer.KEY_DISCOUNT, c_discount));
      values.add(new DoubleValue(Customer.KEY_BALANCE, c_balance));
      values.add(new DoubleValue(Customer.KEY_YTD_PAYMENT, c_ytd_payment));
      values.add(new IntValue(Customer.KEY_PAYMENT_CNT, c_payment_cnt));
      values.add(new IntValue(Customer.KEY_DELIVERY_CNT, c_delivery_cnt));
      values.add(new BigIntValue(Customer.KEY_SINCE, c_since.getTime()));
      return values;
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key key = createPartitionKey();
      ArrayList<Value<?>> values = createValues();
      insert(manager, TABLE_NAME, key, values);
    }

    public String getFirstName() {
      return c_first;
    }

    public String getLastName() {
      return c_last;
    }
  }

  public static class CustomerSecondary extends TPCCRecordBase {
    public static final String TABLE_NAME = "customer_secondary";
    public static final String KEY_WAREHOUSE_ID = "c_w_id";
    public static final String KEY_DISTRICT_ID = "c_d_id";
    public static final String KEY_LAST = "c_last";
    public static final String KEY_FIRST = "c_first";
    public static final String KEY_CUSTOMER_ID = "c_id";

    private int c_w_id;
    private int c_d_id;
    private String c_last;
    private String c_first;
    private int c_id;

    public CustomerSecondary(int warehouseId, int districtId, String last, String first,
        int customerId) {
      c_w_id = warehouseId;
      c_d_id = districtId;
      c_last = last;
      c_first = first;
      c_id = customerId;
    }

    public CustomerSecondary(CSVRecord record) {
      c_w_id = Integer.parseInt(record.get(CustomerSecondary.KEY_WAREHOUSE_ID));
      c_d_id = Integer.parseInt(record.get(CustomerSecondary.KEY_DISTRICT_ID));
      c_last = record.get(CustomerSecondary.KEY_LAST);
      c_first = record.get(CustomerSecondary.KEY_FIRST);
      c_id = Integer.parseInt(record.get(CustomerSecondary.KEY_CUSTOMER_ID));
    }

    public static Key createPartitionKey(int warehouseId, int districtId, String lastName) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
      keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
      keys.add(new TextValue(KEY_LAST, lastName));
      return new Key(keys);
    }

    public static Key createClusteringKey(String firstName, int customerId) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new TextValue(KEY_FIRST, firstName));
      keys.add(new IntValue(KEY_CUSTOMER_ID, customerId));
      return new Key(keys);
    }

    public Key createPartitionKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, c_w_id));
      keys.add(new IntValue(KEY_DISTRICT_ID, c_d_id));
      keys.add(new TextValue(KEY_LAST, c_last));
      return new Key(keys);
    }

    public Key createClusteringKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new TextValue(KEY_FIRST, c_first));
      keys.add(new IntValue(KEY_CUSTOMER_ID, c_id));
      return new Key(keys);
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key partitionKey = createPartitionKey();
      Key clusteringKey = createClusteringKey();
      insert(manager, TABLE_NAME, partitionKey, clusteringKey);
    }
  }

  public static class District extends TPCCRecordBase {
    public static final String TABLE_NAME = "district";
    public static final String KEY_WAREHOUSE_ID = "d_w_id";
    public static final String KEY_ID = "d_id";
    public static final String KEY_NAME = "d_name";
    public static final String KEY_STREET_1 = "d_street_1";
    public static final String KEY_STREET_2 = "d_street_2";
    public static final String KEY_CITY = "d_city";
    public static final String KEY_STATE = "d_state";
    public static final String KEY_ZIP = "d_zip";
    public static final String KEY_TAX = "d_tax";
    public static final String KEY_YTD = "d_ytd";
    public static final String KEY_NEXT_O_ID = "d_next_o_id";

    public static final int CUSTOMERS = 3000;
    public static final int ORDERS = 3000;
    public static final int MIN_NAME = 6;
    public static final int MAX_NAME = 10;

    private int d_w_id;
    private int d_id;
    private String d_name;
    private String d_street_1;
    private String d_street_2;
    private String d_city;
    private String d_state;
    private String d_zip;
    private double d_tax;
    private double d_ytd;
    private int d_next_o_id;

    public District(int warehouseId, int districtId) {
      d_w_id = warehouseId;
      d_id = districtId;
      d_name = TPCCUtil.randomAlphaString(MIN_NAME, MAX_NAME);
      d_street_1 = TPCCUtil.randomAlphaString(Address.MIN_STREET, Address.MAX_STREET);
      d_street_2 = TPCCUtil.randomAlphaString(Address.MIN_STREET, Address.MAX_STREET);
      d_city = TPCCUtil.randomAlphaString(Address.MIN_CITY, Address.MAX_CITY);
      d_state = TPCCUtil.randomAlphaString(Address.STATE_SIZE);
      d_zip = TPCCUtil.randomNumberString(Address.ZIP_SIZE) + "11111";
      d_tax = TPCCUtil.randomDouble(0, 2000, 10000);
      d_ytd = 30000.00;
      d_next_o_id = 3001;
    }

    public District(CSVRecord record) {
      d_w_id = Integer.parseInt(record.get(District.KEY_WAREHOUSE_ID));
      d_id = Integer.parseInt(record.get(District.KEY_ID));
      d_name = record.get(District.KEY_NAME);
      d_street_1 = record.get(District.KEY_STREET_1);
      d_street_2 = record.get(District.KEY_STREET_2);
      d_city = record.get(District.KEY_CITY);
      d_state = record.get(District.KEY_STATE);
      d_zip = record.get(District.KEY_ZIP);
      d_tax = Double.parseDouble(record.get(District.KEY_TAX));
      d_ytd = Double.parseDouble(record.get(District.KEY_YTD));
      d_next_o_id = Integer.parseInt(record.get(District.KEY_NEXT_O_ID));
    }

    public static Key createPartitionKey(int warehouseId, int districtId) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
      keys.add(new IntValue(KEY_ID, districtId));
      return new Key(keys);
    }

    public Key createPartitionKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, d_w_id));
      keys.add(new IntValue(KEY_ID, d_id));
      return new Key(keys);
    }

    public ArrayList<Value<?>> createValues() {
      ArrayList<Value<?>> values = new ArrayList<Value<?>>();
      values.add(new TextValue(District.KEY_NAME, d_name));
      values.add(new TextValue(District.KEY_STREET_1, d_street_1));
      values.add(new TextValue(District.KEY_STREET_2, d_street_2));
      values.add(new TextValue(District.KEY_CITY, d_city));
      values.add(new TextValue(District.KEY_STATE, d_state));
      values.add(new TextValue(District.KEY_ZIP, d_zip));
      values.add(new DoubleValue(District.KEY_TAX, d_tax));
      values.add(new DoubleValue(District.KEY_YTD, d_ytd));
      values.add(new IntValue(District.KEY_NEXT_O_ID, d_next_o_id));
      return values;
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key key = createPartitionKey();
      ArrayList<Value<?>> values = createValues();
      insert(manager, TABLE_NAME, key, values);
    }
  }

  public static class History extends TPCCRecordBase {
    public static final String TABLE_NAME = "history";
    public static final String KEY_ID = "h_id";
    public static final String KEY_CUSTOMER_ID = "h_c_id";
    public static final String KEY_CUSTOMER_DID = "h_c_d_id";
    public static final String KEY_CUSTOMER_WID = "h_c_w_id";
    public static final String KEY_DISTRICT_ID = "h_d_id";
    public static final String KEY_WAREHOUSE_ID = "h_w_id";
    public static final String KEY_DATE = "h_date";
    public static final String KEY_AMOUNT = "h_amount";
    public static final String KEY_DATA = "h_data";

    public static final int MIN_DATA = 12;
    public static final int MAX_DATA = 24;

    private String h_id;
    private int h_c_id;
    private int h_c_d_id;
    private int h_c_w_id;
    private int h_d_id;
    private int h_w_id;
    private Date h_date;
    private double h_amount;
    private String h_data;

    public History(int c_id, int c_d_id, int c_w_id, int d_id, int w_id, Date date) {
      h_id = UUID.randomUUID().toString();
      h_c_id = c_id;
      h_c_d_id = c_d_id;
      h_c_w_id = c_w_id;
      h_d_id = d_id;
      h_w_id = w_id;
      h_date = date;
      h_amount = 10.00;
      h_data = TPCCUtil.randomAlphaString(MIN_DATA, MAX_DATA);
    }

    public History(int c_id, int c_d_id, int c_w_id, int d_id, int w_id, Date date, double amount,
        String data) {
      h_id = UUID.randomUUID().toString();
      h_c_id = c_id;
      h_c_d_id = c_d_id;
      h_c_w_id = c_w_id;
      h_d_id = d_id;
      h_w_id = w_id;
      h_date = date;
      h_amount = amount;
      h_data = data;
    }

    public History(CSVRecord record) throws ParseException {
      h_id = UUID.randomUUID().toString();
      h_c_id = Integer.parseInt(record.get(History.KEY_CUSTOMER_ID));
      h_c_d_id = Integer.parseInt(record.get(History.KEY_CUSTOMER_DID));
      h_c_w_id = Integer.parseInt(record.get(History.KEY_CUSTOMER_WID));
      h_d_id = Integer.parseInt(record.get(History.KEY_DISTRICT_ID));
      h_w_id = Integer.parseInt(record.get(History.KEY_WAREHOUSE_ID));
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      try {
        h_date = dateFormat.parse(record.get(History.KEY_DATE));
      } catch (ParseException e) {
        throw e;
      }
      h_amount = Double.parseDouble(record.get(History.KEY_AMOUNT));
      h_data = record.get(History.KEY_DATA);
    }

    public Key createPartitionKey() {
      return new Key(History.KEY_ID, h_id);
    }

    public ArrayList<Value<?>> createValues() {
      ArrayList<Value<?>> values = new ArrayList<Value<?>>();
      values.add(new IntValue(History.KEY_CUSTOMER_ID, h_c_id));
      values.add(new IntValue(History.KEY_CUSTOMER_DID, h_c_d_id));
      values.add(new IntValue(History.KEY_CUSTOMER_WID, h_c_w_id));
      values.add(new IntValue(History.KEY_DISTRICT_ID, h_d_id));
      values.add(new IntValue(History.KEY_WAREHOUSE_ID, h_w_id));
      values.add(new BigIntValue(History.KEY_DATE, h_date.getTime()));
      values.add(new DoubleValue(History.KEY_AMOUNT, h_amount));
      values.add(new TextValue(History.KEY_DATA, h_data));
      return values;
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key key = createPartitionKey();
      ArrayList<Value<?>> values = createValues();
      insert(manager, TABLE_NAME, key, values);
    }
  }

  public static class Item extends TPCCRecordBase {
    public static final String TABLE_NAME = "item";
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

    private int i_id;
    private String i_name;
    private double i_price;
    private String i_data;
    private int i_im_id;

    public Item(int itemId) {
      i_id = itemId;
      i_name = TPCCUtil.randomAlphaString(MIN_NAME, MAX_NAME);
      i_price = TPCCUtil.randomDouble(100, 1000, 100);
      i_data = TPCCUtil.getRandomStringWithOriginal(MIN_DATA, MAX_DATA, 10);
      i_im_id = TPCCUtil.randomInt(1, 10000);
    }

    public Item(CSVRecord record) {
      i_id = Integer.parseInt(record.get(Item.KEY_ID));
      i_name = record.get(Item.KEY_NAME);
      i_price = Double.parseDouble(record.get(Item.KEY_PRICE));
      i_data = record.get(Item.KEY_DATA);
      i_im_id = Integer.parseInt(record.get(Item.KEY_IM_ID));
    }

    public static Key createPartitionKey(int itemId) {
      return new Key(Item.KEY_ID, itemId);
    }

    public Key createPartitionKey() {
      return new Key(Item.KEY_ID, i_id);
    }

    public ArrayList<Value<?>> createValues() {
      ArrayList<Value<?>> values = new ArrayList<Value<?>>();
      values.add(new TextValue(Item.KEY_NAME, i_name));
      values.add(new DoubleValue(Item.KEY_PRICE, i_price));
      values.add(new TextValue(Item.KEY_DATA, i_data));
      values.add(new IntValue(Item.KEY_IM_ID, i_im_id));
      return values;
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key key = createPartitionKey();
      ArrayList<Value<?>> values = createValues();
      insert(manager, TABLE_NAME, key, values);
    }
  }

  public static class NewOrder extends TPCCRecordBase {
    public static final String TABLE_NAME = "new_order";
    public static final String KEY_WAREHOUSE_ID = "no_w_id";
    public static final String KEY_DISTRICT_ID = "no_d_id";
    public static final String KEY_ORDER_ID = "no_o_id";

    private int no_w_id;
    private int no_d_id;
    private int no_o_id;

    public NewOrder(int warehouseId, int districtId, int orderId) {
      no_w_id = warehouseId;
      no_d_id = districtId;
      no_o_id = orderId;
    }

    public NewOrder(CSVRecord record) {
      no_w_id = Integer.parseInt(record.get(NewOrder.KEY_WAREHOUSE_ID));
      no_d_id = Integer.parseInt(record.get(NewOrder.KEY_DISTRICT_ID));
      no_o_id = Integer.parseInt(record.get(NewOrder.KEY_ORDER_ID));
    }

    public static Key createPartitionKey(int warehouseId, int districtId) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
      keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
      return new Key(keys);
    }

    public static Key createClusteringKey(int orderId) {
      return new Key(KEY_ORDER_ID, orderId);
    }

    public Key createPartitionKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, no_w_id));
      keys.add(new IntValue(KEY_DISTRICT_ID, no_d_id));
      return new Key(keys);
    }

    public Key createClusteringKey() {
      return new Key(KEY_ORDER_ID, no_o_id);
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key partitionKey = createPartitionKey();
      Key clusteringKey = createClusteringKey();
      insert(manager, TABLE_NAME, partitionKey, clusteringKey);
    }
  }

  public static class Order extends TPCCRecordBase {
    public static final String TABLE_NAME = "oorder";
    public static final String KEY_WAREHOUSE_ID = "o_w_id";
    public static final String KEY_DISTRICT_ID = "o_d_id";
    public static final String KEY_ID = "o_id";
    public static final String KEY_CUSTOMER_ID = "o_c_id";
    public static final String KEY_CARRIER_ID = "o_carrier_id";
    public static final String KEY_OL_CNT = "o_ol_cnt";
    public static final String KEY_ALL_LOCAL = "o_all_local";
    public static final String KEY_ENTRY_D = "o_entry_d";

    private int o_w_id;
    private int o_d_id;
    private int o_id;
    private int o_c_id;
    private int o_carrier_id;
    private int o_ol_cnt;
    private int o_all_local;
    private Date o_entry_d;

    public Order(int warehouseId, int districtId, int orderId, int customerId, int carrierId,
        int number, int local, Date date) {
      o_w_id = warehouseId;
      o_d_id = districtId;
      o_id = orderId;
      o_c_id = customerId;
      o_carrier_id = carrierId;
      o_ol_cnt = number;
      o_all_local = local;
      o_entry_d = date;
    }

    public Order(int warehouseId, int districtId, int orderId, int customerId, Date date) {
      o_w_id = warehouseId;
      o_d_id = districtId;
      o_id = orderId;
      o_c_id = customerId;
      if (o_id < 2101) {
        o_carrier_id = TPCCUtil.randomInt(1, 10);
      } else {
        o_carrier_id = 0;
      }
      o_ol_cnt = TPCCUtil.randomInt(OrderLine.MIN_PER_ORDER, OrderLine.MAX_PER_ORDER);
      o_all_local = 1;
      o_entry_d = date;
    }

    public Order(CSVRecord record) throws ParseException {
      o_w_id = Integer.parseInt(record.get(Order.KEY_WAREHOUSE_ID));
      o_d_id = Integer.parseInt(record.get(Order.KEY_DISTRICT_ID));
      o_id = Integer.parseInt(record.get(Order.KEY_ID));
      o_c_id = Integer.parseInt(record.get(Order.KEY_CUSTOMER_ID));
      if (!record.get(Order.KEY_CARRIER_ID).isEmpty()
          && !record.get(Order.KEY_CARRIER_ID).equals("\\N")) {
        o_carrier_id = Integer.parseInt(record.get(Order.KEY_CARRIER_ID));
      } else {
        o_carrier_id = 0;
      }
      o_ol_cnt = Integer.parseInt(record.get(Order.KEY_OL_CNT));
      o_all_local = Integer.parseInt(record.get(Order.KEY_ALL_LOCAL));
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      try {
        o_entry_d = dateFormat.parse(record.get(Order.KEY_ENTRY_D));
      } catch (ParseException e) {
        throw e;
      }
    }

    public static Key createPartitionKey(int warehouseId, int districtId) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
      keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
      return new Key(keys);
    }

    public static Key createClusteringKey(int orderId) {
      return new Key(KEY_ID, orderId);
    }

    public Key createPartitionKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, o_w_id));
      keys.add(new IntValue(KEY_DISTRICT_ID, o_d_id));
      return new Key(keys);
    }

    public Key createClusteringKey() {
      return new Key(KEY_ID, o_id);
    }

    public ArrayList<Value<?>> createValues() {
      ArrayList<Value<?>> values = new ArrayList<Value<?>>();
      values.add(new IntValue(Order.KEY_CUSTOMER_ID, o_c_id));
      if (o_carrier_id > 0) {
        values.add(new IntValue(Order.KEY_CARRIER_ID, o_carrier_id));
      }
      values.add(new IntValue(Order.KEY_OL_CNT, o_ol_cnt));
      values.add(new IntValue(Order.KEY_ALL_LOCAL, o_all_local));
      values.add(new BigIntValue(Order.KEY_ENTRY_D, o_entry_d.getTime()));
      return values;
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key partitionKey = createPartitionKey();
      Key clusteringKey = createClusteringKey();
      ArrayList<Value<?>> values = createValues();
      insert(manager, TABLE_NAME, partitionKey, clusteringKey, values);
    }

    public int getOrderLineCount() {
      return o_ol_cnt;
    }
  }

  public static class OrderSecondary extends TPCCRecordBase {
    public static final String TABLE_NAME = "order_secondary";
    public static final String KEY_WAREHOUSE_ID = "o_w_id";
    public static final String KEY_DISTRICT_ID = "o_d_id";
    public static final String KEY_CUSTOMER_ID = "o_c_id";
    public static final String KEY_ORDER_ID = "o_id";

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      // TODO: TPCC-FULL
    }
  }

  public static class OrderLine extends TPCCRecordBase {
    public static final String TABLE_NAME = "order_line";
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

    private int ol_w_id;
    private int ol_d_id;
    private int ol_o_id;
    private int ol_number;
    private int ol_i_id;
    private Date ol_delivery_d;
    private double ol_amount;
    private int ol_supply_w_id;
    private int ol_quantity;
    private String ol_dist_info;

    public OrderLine(int warehouseId, int districtId, int orderId, int number,
        int supplyWarehouseId, double amount, int quantity, int itemId, String info) {
      ol_w_id = warehouseId;
      ol_d_id = districtId;
      ol_o_id = orderId;
      ol_number = number;
      ol_i_id = itemId;
      ol_supply_w_id = supplyWarehouseId;
      ol_delivery_d = null;
      ol_amount = amount;
      ol_quantity = quantity;
      ol_dist_info = info;
    }

    public OrderLine(int warehouseId, int districtId, int orderId, int number,
        int supplyWarehouseId, int itemId, Date date) {
      ol_w_id = warehouseId;
      ol_d_id = districtId;
      ol_o_id = orderId;
      ol_number = number;
      ol_i_id = itemId;
      ol_supply_w_id = supplyWarehouseId;
      if (ol_o_id < 2101) {
        ol_delivery_d = date;
        ol_amount = 0.00;
      } else {
        ol_delivery_d = null;
        ol_amount = TPCCUtil.randomDouble(1, 999999, 100);
      }
      ol_quantity = 5;
      ol_dist_info = TPCCUtil.randomAlphaString(DIST_INFO_SIZE);
    }

    public OrderLine(CSVRecord record) throws ParseException {
      ol_w_id = Integer.parseInt(record.get(OrderLine.KEY_WAREHOUSE_ID));
      ol_d_id = Integer.parseInt(record.get(OrderLine.KEY_DISTRICT_ID));
      ol_o_id = Integer.parseInt(record.get(OrderLine.KEY_ORDER_ID));
      ol_number = Integer.parseInt(record.get(OrderLine.KEY_NUMBER));
      ol_i_id = Integer.parseInt(record.get(OrderLine.KEY_ITEM_ID));
      if (!record.get(OrderLine.KEY_DELIVERY_D).isEmpty()
          && !record.get(OrderLine.KEY_DELIVERY_D).equals("\\N")) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
          ol_delivery_d = dateFormat.parse(record.get(OrderLine.KEY_DELIVERY_D));
        } catch (ParseException e) {
          throw e;
        }
      } else {
        ol_delivery_d = null;
      }
      ol_amount = Double.parseDouble(record.get(OrderLine.KEY_AMOUNT));
      ol_supply_w_id = Integer.parseInt(record.get(OrderLine.KEY_SUPPLY_W_ID));
      ol_quantity = Integer.parseInt(record.get(OrderLine.KEY_QUANTITY));
      ol_dist_info = record.get(OrderLine.KEY_DIST_INFO);
    }

    public static Key createPartitionKey(int warehouseId, int districtId) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
      keys.add(new IntValue(KEY_DISTRICT_ID, districtId));
      return new Key(keys);
    }

    public static Key createClusteringKey(int orderId, int orderLineNumber) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_ORDER_ID, orderId));
      keys.add(new IntValue(KEY_NUMBER, orderLineNumber));
      return new Key(keys);
    }

    public Key createPartitionKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, ol_w_id));
      keys.add(new IntValue(KEY_DISTRICT_ID, ol_d_id));
      return new Key(keys);
    }

    public Key createClusteringKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_ORDER_ID, ol_o_id));
      keys.add(new IntValue(KEY_NUMBER, ol_number));
      return new Key(keys);
    }

    public ArrayList<Value<?>> createValues() {
      ArrayList<Value<?>> values = new ArrayList<Value<?>>();
      values.add(new IntValue(OrderLine.KEY_ITEM_ID, ol_i_id));
      if (ol_delivery_d != null) {
        values.add(new BigIntValue(OrderLine.KEY_DELIVERY_D, ol_delivery_d.getTime()));
      }
      values.add(new DoubleValue(OrderLine.KEY_AMOUNT, ol_amount));
      values.add(new IntValue(OrderLine.KEY_SUPPLY_W_ID, ol_supply_w_id));
      values.add(new IntValue(OrderLine.KEY_QUANTITY, ol_quantity));
      values.add(new TextValue(OrderLine.KEY_DIST_INFO, ol_dist_info));
      return values;
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key partitionKey = createPartitionKey();
      Key clusteringKey = createClusteringKey();
      ArrayList<Value<?>> values = createValues();
      insert(manager, TABLE_NAME, partitionKey, clusteringKey, values);
    }
  }

  public static class Stock extends TPCCRecordBase {
    public static final String TABLE_NAME = "stock";
    public static final String KEY_WAREHOUSE_ID = "s_w_id";
    public static final String KEY_ITEM_ID = "s_i_id";
    public static final String KEY_QUANTITY = "s_quantity";
    public static final String KEY_YTD = "s_ytd";
    public static final String KEY_ORDER_CNT = "s_order_cnt";
    public static final String KEY_REMOTE_CNT = "s_remote_cnt";
    public static final String KEY_DATA = "s_data";
    public static final String KEY_DIST_PREFIX = "s_dist_";
    public static final String KEY_DISTRICT01 = "s_dist_01";
    public static final String KEY_DISTRICT02 = "s_dist_02";
    public static final String KEY_DISTRICT03 = "s_dist_03";
    public static final String KEY_DISTRICT04 = "s_dist_04";
    public static final String KEY_DISTRICT05 = "s_dist_05";
    public static final String KEY_DISTRICT06 = "s_dist_06";
    public static final String KEY_DISTRICT07 = "s_dist_07";
    public static final String KEY_DISTRICT08 = "s_dist_08";
    public static final String KEY_DISTRICT09 = "s_dist_09";
    public static final String KEY_DISTRICT10 = "s_dist_10";

    public static final int MIN_DATA = 26;
    public static final int MAX_DATA = 50;
    public static final int DIST_SIZE = 24;

    private int s_w_id;
    private int s_i_id;
    private int s_quantity;
    private double s_ytd;
    private int s_order_cnt;
    private int s_remote_cnt;
    private String s_data;
    private String[] s_dist = new String[10];

    public Stock(int warehouseId, int itemId, int quantity, double ytd, int orderCount,
        int remoteCount) {
      s_w_id = warehouseId;
      s_i_id = itemId;
      s_quantity = quantity;
      s_ytd = ytd;
      s_order_cnt = orderCount;
      s_remote_cnt = remoteCount;
    }

    public Stock(int warehouseId, int itemId) {
      s_w_id = warehouseId;
      s_i_id = itemId;
      s_quantity = TPCCUtil.randomInt(10, 100);
      s_ytd = 0;
      s_order_cnt = 0;
      s_remote_cnt = 0;
      s_data = TPCCUtil.getRandomStringWithOriginal(MIN_DATA, MAX_DATA, 10);
      for (int i = 0; i < 10; i++) {
        s_dist[i] = TPCCUtil.randomAlphaString(DIST_SIZE);
      }
    }

    public Stock(CSVRecord record) {
      s_w_id = Integer.parseInt(record.get(Stock.KEY_WAREHOUSE_ID));
      s_i_id = Integer.parseInt(record.get(Stock.KEY_ITEM_ID));
      s_quantity = Integer.parseInt(record.get(Stock.KEY_QUANTITY));
      s_ytd = Double.parseDouble(record.get(Stock.KEY_YTD));
      s_order_cnt = Integer.parseInt(record.get(Stock.KEY_ORDER_CNT));
      s_remote_cnt = Integer.parseInt(record.get(Stock.KEY_REMOTE_CNT));
      s_data = record.get(Stock.KEY_DATA);
      for (int i = 0; i < 10; i++) {
        s_dist[i] = record.get(Stock.KEY_DIST_PREFIX + String.format("%02d", i + 1));
      }
    }

    public static Key createPartitionKey(int warehouseId, int itemId) {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, warehouseId));
      keys.add(new IntValue(KEY_ITEM_ID, itemId));
      return new Key(keys);
    }

    public Key createPartitionKey() {
      ArrayList<Value<?>> keys = new ArrayList<Value<?>>();
      keys.add(new IntValue(KEY_WAREHOUSE_ID, s_w_id));
      keys.add(new IntValue(KEY_ITEM_ID, s_i_id));
      return new Key(keys);
    }

    public ArrayList<Value<?>> createValues() {
      ArrayList<Value<?>> values = new ArrayList<Value<?>>();
      values.add(new IntValue(Stock.KEY_QUANTITY, s_quantity));
      values.add(new DoubleValue(Stock.KEY_YTD, s_ytd));
      values.add(new IntValue(Stock.KEY_ORDER_CNT, s_order_cnt));
      values.add(new IntValue(Stock.KEY_REMOTE_CNT, s_remote_cnt));
      if (s_data != null) {
        values.add(new TextValue(Stock.KEY_DATA, s_data));
      }
      for (int i = 0; i < 10; i++) {
        if (s_dist[i] != null) {
          values
              .add(new TextValue(Stock.KEY_DIST_PREFIX + String.format("%02d", i + 1), s_dist[i]));
        }
      }
      return values;
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key key = createPartitionKey();
      ArrayList<Value<?>> values = createValues();
      insert(manager, TABLE_NAME, key, values);
    }
  }

  public static class Warehouse extends TPCCRecordBase {
    public static final String TABLE_NAME = "warehouse";
    public static final String KEY_ID = "w_id";
    public static final String KEY_NAME = "w_name";
    public static final String KEY_STREET_1 = "w_street_1";
    public static final String KEY_STREET_2 = "w_street_2";
    public static final String KEY_CITY = "w_city";
    public static final String KEY_STATE = "w_state";
    public static final String KEY_ZIP = "w_zip";
    public static final String KEY_TAX = "w_tax";
    public static final String KEY_YTD = "w_ytd";

    public static final int DISTRICTS = 10;
    public static final int STOCKS = 100000;
    public static final int MIN_NAME = 6;
    public static final int MAX_NAME = 10;

    private int w_id;
    private String w_name;
    private String w_street_1;
    private String w_street_2;
    private String w_city;
    private String w_state;
    private String w_zip;
    private double w_tax;
    private double w_ytd;

    public Warehouse(int warehouseId) {
      w_id = warehouseId;
      w_name = TPCCUtil.randomAlphaString(MIN_NAME, MAX_NAME);
      w_street_1 = TPCCUtil.randomAlphaString(Address.MIN_STREET, Address.MAX_STREET);
      w_street_2 = TPCCUtil.randomAlphaString(Address.MIN_STREET, Address.MAX_STREET);
      w_city = TPCCUtil.randomAlphaString(Address.MIN_CITY, Address.MAX_CITY);
      w_state = TPCCUtil.randomAlphaString(Address.STATE_SIZE);
      w_zip = TPCCUtil.randomNumberString(Address.ZIP_SIZE) + "11111";
      w_tax = TPCCUtil.randomDouble(0, 2000, 10000);
      w_ytd = 300000.00;
    }

    public Warehouse(CSVRecord record) {
      w_id = Integer.parseInt(record.get(Warehouse.KEY_ID));
      w_name = record.get(Warehouse.KEY_NAME);
      w_street_1 = record.get(Warehouse.KEY_STREET_1);
      w_street_2 = record.get(Warehouse.KEY_STREET_2);
      w_city = record.get(Warehouse.KEY_CITY);
      w_state = record.get(Warehouse.KEY_STATE);
      w_zip = record.get(Warehouse.KEY_ZIP);
      w_tax = Double.parseDouble(record.get(Warehouse.KEY_TAX));
      w_ytd = Double.parseDouble(record.get(Warehouse.KEY_YTD));
    }

    public static Key createPartitionKey(int warehouseId) {
      return new Key(KEY_ID, warehouseId);
    }

    public Key createPartitionKey() {
      return new Key(KEY_ID, w_id);
    }

    public ArrayList<Value<?>> createValues() {
      ArrayList<Value<?>> values = new ArrayList<Value<?>>();
      values.add(new TextValue(Warehouse.KEY_NAME, w_name));
      values.add(new TextValue(Warehouse.KEY_STREET_1, w_street_1));
      values.add(new TextValue(Warehouse.KEY_STREET_2, w_street_2));
      values.add(new TextValue(Warehouse.KEY_CITY, w_city));
      values.add(new TextValue(Warehouse.KEY_STATE, w_state));
      values.add(new TextValue(Warehouse.KEY_ZIP, w_zip));
      values.add(new DoubleValue(Warehouse.KEY_TAX, w_tax));
      values.add(new DoubleValue(Warehouse.KEY_YTD, w_ytd));
      return values;
    }

    public void insert(DistributedTransactionManager manager) throws TransactionException {
      Key key = createPartitionKey();
      ArrayList<Value<?>> values = createValues();
      insert(manager, TABLE_NAME, key, values);
    }
  }
}
