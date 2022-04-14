package com.scalar.db.benchmarks.tpcc;

import javax.annotation.concurrent.Immutable;

@Immutable
public class TpccConfig {
  private int rateNewOrder = 50;
  private int ratePayment = 50;
  private int rateOrderStatus = 0;
  private int rateDelivery = 0;
  private int rateStockLevel = 0;
  private int numWarehouse = 1;

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

    Builder() {
      rateNewOrder = 50;
      ratePayment = 50;
      rateOrderStatus = 0;
      rateDelivery = 0;
      rateStockLevel = 0;
      numWarehouse = 1;
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

    /**
     * Builds a {@code TpccConfig} with the specified parameter.
     *
     * @return a {@code TpccConfig} object
     */
    public TpccConfig build() {
      int total = rateNewOrder + ratePayment + rateOrderStatus + rateDelivery + rateStockLevel;
      if (total != 100) {
        throw new IllegalArgumentException("Total rate must be 100.");
      }
      if (rateOrderStatus != 0 || rateDelivery != 0 || rateStockLevel != 0) {
        throw new IllegalArgumentException("Currrently TPC-C NP only.");
      }
      return new TpccConfig(this);
    }
  }
}
