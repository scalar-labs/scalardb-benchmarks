package com.scalar.db.benchmarks.tpcc;

public class TPCCConfig {
  private int rateNewOrder = 50;
  private int ratePayment = 50;
  private int rateOrderStatus = 0;
  private int rateDelivery = 0;
  private int numWarehouse = 1;

  public TPCCConfig(int warehouse, int payment) {
    numWarehouse = warehouse;
    ratePayment = payment;
  }

  public int getRateNewOrder() {
    return rateNewOrder;
  }

  public void setRateNewOrder(int rateNewOrder) {
    this.rateNewOrder = rateNewOrder;
  }

  public int getRatePayment() {
    return ratePayment;
  }

  public void setRatePayment(int ratePayment) {
    this.ratePayment = ratePayment;
  }

  public int getRateOrderStatus() {
    return rateOrderStatus;
  }

  public void setRateOrderStatus(int rateOrderStatus) {
    this.rateOrderStatus = rateOrderStatus;
  }

  public int getRateDelivery() {
    return rateDelivery;
  }

  public void setRateDelivery(int rateDelivery) {
    this.rateDelivery = rateDelivery;
  }

  public int getNumWarehouse() {
    return numWarehouse;
  }

  public void setNumWarehouse(int numWarehouse) {
    this.numWarehouse = numWarehouse;
  }
}
