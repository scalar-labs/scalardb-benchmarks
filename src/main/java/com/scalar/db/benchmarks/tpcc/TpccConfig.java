package com.scalar.db.benchmarks.tpcc;

import javax.annotation.concurrent.Immutable;

@Immutable
public class TpccConfig {
  private final int rateNewOrder;
  private final int ratePayment;
  private final int rateOrderStatus;
  private final int rateDelivery;
  private final int rateStockLevel;
  private final int numWarehouse;
  private final int backoff;
  private final boolean isNpOnly;

  /**
   * Constructs a {@code TpccConfig} with the specified {@link TpccConfig.Builder}.
   *
   * @param builder a {@code TpccConfig.Builder} object
   */
  private TpccConfig(Builder builder) {
    this.rateNewOrder = builder.rateNewOrder;
    this.ratePayment = builder.ratePayment;
    this.rateOrderStatus = builder.rateOrderStatus;
    this.rateDelivery = builder.rateDelivery;
    this.rateStockLevel = builder.rateStockLevel;
    this.numWarehouse = builder.numWarehouse;
    this.backoff = builder.backoff;
    this.isNpOnly = builder.isNpOnly;
  }

  public int getRateNewOrder() {
    return rateNewOrder;
  }

  public int getRatePayment() {
    return ratePayment;
  }

  public int getRateOrderStatus() {
    return rateOrderStatus;
  }

  public int getRateDelivery() {
    return rateDelivery;
  }

  public int getRateStockLevel() {
    return rateStockLevel;
  }

  public int getNumWarehouse() {
    return numWarehouse;
  }

  public int getBackoff() {
    return backoff;
  }

  public boolean isNpOnly() {
    return isNpOnly;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private int rateNewOrder;
    private int ratePayment;
    private int rateOrderStatus;
    private int rateDelivery;
    private int rateStockLevel;
    private int numWarehouse;
    private int backoff;
    private boolean isNpOnly;

    private Builder() {
      rateNewOrder = 45;
      ratePayment = 43;
      rateOrderStatus = 4;
      rateDelivery = 4;
      rateStockLevel = 4;
      numWarehouse = 1;
      backoff = 0;
      isNpOnly = false;
    }

    public Builder fullMix() {
      this.rateNewOrder = 45;
      this.ratePayment = 43;
      this.rateOrderStatus = 4;
      this.rateDelivery = 4;
      this.rateStockLevel = 4;
      return this;
    }

    public Builder npOnly() {
      this.rateNewOrder = 50;
      this.ratePayment = 50;
      this.rateOrderStatus = 0;
      this.rateDelivery = 0;
      this.rateStockLevel = 0;
      return this;
    }

    public Builder rateNewOrder(int rateNewOrder) {
      this.rateNewOrder = rateNewOrder;
      return this;
    }
     
    public Builder ratePayment(int ratePayment) {
      this.ratePayment = ratePayment;
      return this;
    }

    public Builder rateOrderStatus(int rateOrderStatus) {
      this.rateOrderStatus = rateOrderStatus;
      return this;
    }

    public Builder rateDelivery(int rateDelivery) {
      this.rateDelivery = rateDelivery;
      return this;
    }

    public Builder rateStockLevel(int rateStockLevel) {
      this.rateStockLevel = rateStockLevel;
      return this;
    }

    public Builder numWarehouse(int numWarehouse) {
      this.numWarehouse = numWarehouse;
      return this;
    }

    public Builder backoff(int backoff) {
      this.backoff = backoff;
      return this;
    }

    /**
     * Builds a {@code TpccConfig} with the specified parameter.
     *
     * @return a {@code TpccConfig} object
     */
    public TpccConfig build() {
      int total = rateNewOrder + ratePayment + rateOrderStatus + rateDelivery + rateStockLevel;
      if (total != 100) {
        throw new IllegalStateException("Total rate must be 100.");
      }
      if (rateNewOrder == ratePayment) {
        isNpOnly = true;
      }
      return new TpccConfig(this);
    }
  }
}
