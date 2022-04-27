package com.scalar.db.benchmarks.tpcc;

import com.scalar.db.benchmarks.tpcc.table.District;
import com.scalar.db.benchmarks.tpcc.table.Item;
import java.util.Random;

public class TpccUtil {
  private static final Random r = new Random();

  // for customer last name
  public static final String[] NAME_TOKENS =
      {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

  // for non-uniform random
  public static final int CUSTOMER_LASTNAME_IN_LOAD = 250;
  public static final int CUSTOMER_LASTNAME_IN_RUN = 150;
  public static final int CUSTOMER_ID = 987;
  public static final int ORDER_LINE_ITEM_ID = 5987;

  /**
   * Returns a customer ID for transaction arguments.
   *
   * @return a customer ID for transaction arguments
   */
  public static int getCustomerId() {
    return nonUniformRandom(1023, 1, District.CUSTOMERS);
  }

  /**
   * Returns an item ID for transaction arguments.
   *
   * @return an item ID for transaction arguments
   */
  public static int getItemId() {
    return nonUniformRandom(8191, 1, Item.ITEMS);
  }

  /**
   * Returns a random {@code String} including "ORIGINAL".
   *
   * @param minLength minimum length of generated string
   * @param maxLength maximum length of generated string
   * @param rate probability of including "ORIGINAL"
   * @return a random {@code String} including "ORIGINAL"
   */
  public static String getRandomStringWithOriginal(int minLength, int maxLength, int rate) {
    int length = randomInt(minLength, maxLength);
    if (TpccUtil.randomInt(0, 99) < rate) {
      int startOriginal = TpccUtil.randomInt(2, length - 8);
      return TpccUtil.randomAlphaString(startOriginal - 1) + "ORIGINAL"
          + TpccUtil.randomAlphaString(length - startOriginal - 9);
    } else {
      return TpccUtil.randomAlphaString(length);
    }
  }

  /**
   * Returns a customer last name {@code String} for transaction argument.
   *
   * @return a customer last name {@code String} for transaction argument
   */
  public static String getNonUniformRandomLastNameForRun() {
    return getLastName(nonUniformRandom(255, 0, 999, false));
  }

  /**
   * Returns a customer last name {@code String} for load.
   *
   * @return a customer last name {@code String} for load
   */
  public static String getNonUniformRandomLastNameForLoad() {
    return getLastName(nonUniformRandom(255, 0, 999, true));
  }

  /**
   * Returns a customer last name {@code String} for load.
   *
   * @param num a number to select name tokens
   * @return a customer last name {@code String} for load
   */
  public static String getLastName(int num) {
    return NAME_TOKENS[num / 100] + NAME_TOKENS[(num / 10) % 10] + NAME_TOKENS[num % 10];
  }

  private static String randomString(int minLength, int maxLength, boolean isNumberOnly) {
    byte[] characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".getBytes();
    int offset = isNumberOnly ? 9 : (characters.length - 1);
    int length = randomInt(minLength, maxLength);
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; ++i) {
      bytes[i] = characters[randomInt(0, offset)];
    }
    return new String(bytes);
  }

  public static String randomAlphaString(int length) {
    return randomString(length, length, false);
  }

  public static String randomAlphaString(int minLength, int maxLength) {
    return randomString(minLength, maxLength, false);
  }

  public static String randomNumberString(int length) {
    return randomNumberString(length, length);
  }

  public static String randomNumberString(int minLength, int maxLength) {
    return randomString(minLength, maxLength, true);
  }

  public static int randomInt(int min, int max) {
    return (int) (r.nextDouble() * (max - min + 1) + min);
  }

  public static double randomDouble(int min, int max, int divider) {
    return randomInt(min, max) / (double) divider;
  }

  public static int nonUniformRandom(int a, int min, int max) {
    return nonUniformRandom(a, min, max, false);
  }

  public static int nonUniformRandom(int a, int min, int max, Boolean isLoad) {
    int c = getConstantForNonUniformRandom(a, isLoad);
    return (((randomInt(0, a) | randomInt(min, max)) + c) % (max - min + 1)) + min;
  }

  private static int getConstantForNonUniformRandom(int a, Boolean isLoad) {
    switch (a) {
      case 255:
        return isLoad ? CUSTOMER_LASTNAME_IN_LOAD : CUSTOMER_LASTNAME_IN_RUN;
      case 1023:
        return CUSTOMER_ID;
      case 8191:
        return ORDER_LINE_ITEM_ID;
      default:
        return 0; // BUG
    }
  }
}
