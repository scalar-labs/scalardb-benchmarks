package com.scalar.db.benchmarks.tpcc;

import java.util.Random;

public class TPCCUtil {
  private static final Random r = new Random();

  // for customer last name
  public static final String[] NAME_TOKENS =
      {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

  // for non-uniform random
  public static final int CUSTOMER_LASTNAME_IN_LOAD = 250;
  public static final int CUSTOMER_LASTNAME_IN_RUN = 150;
  public static final int CUSTOMER_ID = 987;
  public static final int ORDER_LINE_ITEM_ID = 5987;

  public static int getCustomerId() {
    return nonUniformRandom(1023, 1, TPCCTable.District.CUSTOMERS);
  }

  public static String getLastName(int num) {
    return NAME_TOKENS[num / 100] + NAME_TOKENS[(num / 10) % 10] + NAME_TOKENS[num % 10];
  }

  public static int getItemId() {
    // return nonUniformRandom(8191, 1, TPCCTable.Item.ITEMS);
    return nonUniformRandom(8191, 1, 100);
  }

  public static String getRandomStringWithOriginal(int minLength, int maxLength, int rate) {
    int length = randomInt(minLength, maxLength);
    if (TPCCUtil.randomInt(0, 99) < 10) {
      int startOriginal = TPCCUtil.randomInt(2, length - 8);
      return TPCCUtil.randomAlphaString(startOriginal - 1) + "ORIGINAL"
          + TPCCUtil.randomAlphaString(length - startOriginal - 9);
    } else {
      return TPCCUtil.randomAlphaString(length);
    }
  }

  public static String getNonUniformRandomLastNameForRun() {
    return getLastName(nonUniformRandom(255, 0, 999, false));
  }

  public static String getNonUniformRandomLastNameForLoad() {
    return getLastName(nonUniformRandom(255, 0, 999, true));
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
    return randomString(length, length, true);
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

  public static int nonUniformRandom(int A, int min, int max) {
    return nonUniformRandom(A, min, max, false);
  }

  public static int nonUniformRandom(int A, int min, int max, Boolean isLoad) {
    int C = getConstantForNonUniformRandom(A, isLoad);
    return (((randomInt(0, A) | randomInt(min, max)) + C) % (max - min + 1)) + min;
  }

  private static int getConstantForNonUniformRandom(int A, Boolean isLoad) {
    switch (A) {
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
