package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.ArrayList;

public class Address {
  public static final String KEY_STREET_1 = "street_1";
  public static final String KEY_STREET_2 = "street_2";
  public static final String KEY_CITY = "city";
  public static final String KEY_STATE = "state";
  public static final String KEY_ZIP = "zip";

  public static final int MIN_STREET = 10;
  public static final int MAX_STREET = 20;
  public static final int MIN_CITY = 10;
  public static final int MAX_CITY = 20;
  public static final int STATE_SIZE = 2;
  public static final int ZIP_SIZE = 4;

  private String columnPrefix;
  private String street1;
  private String street2;
  private String city;
  private String state;
  private String zip;

  /**
   * Constructs an {@code Address} with specified parameters.
   */
  public Address(String columnPrefix, String street1, String street2,
      String city, String state, String zip) {
    this.columnPrefix = columnPrefix;
    this.street1 = street1;
    this.street2 = street2;
    this.city = city;
    this.state = state;
    this.zip = zip;
  }

  /**
   * Constructs an {@code Address} with data generation.
   */
  public Address(String columnPrefix) {
    this.columnPrefix = columnPrefix;
    this.street1 = TpccUtil.randomAlphaString(MIN_STREET, MAX_STREET);
    this.street2 = TpccUtil.randomAlphaString(MIN_STREET, MAX_STREET);
    this.city = TpccUtil.randomAlphaString(MIN_CITY, MAX_CITY);
    this.state = TpccUtil.randomAlphaString(STATE_SIZE);
    this.zip = TpccUtil.randomNumberString(ZIP_SIZE) + "11111";
  }

  /**
   * Creates an {@code ArrayList} of {@code Value<?>}.
   * 
   * @return an {@code ArrayList} of {@code Value<?>}
   */
  public ArrayList<Value<?>> createValues() {
    ArrayList<Value<?>> values = new ArrayList<Value<?>>();
    values.add(new TextValue(columnPrefix + KEY_STREET_1, street1));
    values.add(new TextValue(columnPrefix + KEY_STREET_2, street2));
    values.add(new TextValue(columnPrefix + KEY_CITY, city));
    values.add(new TextValue(columnPrefix + KEY_STATE, state));
    values.add(new TextValue(columnPrefix + KEY_ZIP, zip));
    return values;
  }
}
