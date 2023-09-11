package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.benchmarks.tpcc.TpccConfig;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.benchmarks.tpcc.table.Customer;
import com.scalar.db.benchmarks.tpcc.table.District;
import com.scalar.db.benchmarks.tpcc.table.History;
import com.scalar.db.benchmarks.tpcc.table.Warehouse;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Date;
import java.util.Optional;

public class PaymentTransaction implements TpccTransaction {
  private final TpccConfig config;
  private final DistributedTransactionManager manager;
  private DistributedTransaction transaction;
  private int warehouseId;
  private int districtId;
  private int customerId;
  private int customerWarehouseId;
  private int customerDistrictId;
  private String customerLastName;
  private boolean byLastName;
  private float paymentAmount;
  private Date date;

  public PaymentTransaction(DistributedTransactionManager manager, TpccConfig config) {
    this.manager = manager;
    this.config = config;
    generate();
  }

  private void generate() {
    int numWarehouse = config.getNumWarehouse();
    warehouseId = TpccUtil.randomInt(1, numWarehouse);
    districtId = TpccUtil.randomInt(1, Warehouse.DISTRICTS);
    paymentAmount = (float) (TpccUtil.randomInt(100, 500000) / 100.0);
    date = new Date();

    int x = TpccUtil.randomInt(1, 100);
    if (x <= 85) {
      // home warehouse
      customerWarehouseId = warehouseId;
      customerDistrictId = districtId;
    } else {
      // remote warehouse
      if (numWarehouse > 1) {
        do {
          customerWarehouseId = TpccUtil.randomInt(1, numWarehouse);
        } while (customerWarehouseId == warehouseId);
      } else {
        customerWarehouseId = warehouseId;
      }
      customerDistrictId = TpccUtil.randomInt(1, Warehouse.DISTRICTS);
    }

    int y = TpccUtil.randomInt(1, 100);
    if (y <= 60) {
      // by last name
      byLastName = true;
      customerLastName = TpccUtil.getNonUniformRandomLastNameForRun();
    } else {
      // by customer id
      byLastName = false;
      customerId = TpccUtil.getCustomerId();
    }
  }

  private String generateCustomerData(
      int warehouseId,
      int districtId,
      int customerId,
      int customerWarehouseId,
      int customerDistrictId,
      double amount,
      String oldData) {
    String data =
        customerId
            + " "
            + customerDistrictId
            + " "
            + customerWarehouseId
            + " "
            + districtId
            + " "
            + warehouseId
            + " "
            + String.format("%7.2f", amount)
            + " | "
            + oldData;
    if (data.length() > 500) {
      data = data.substring(0, 500);
    }
    return data;
  }

  private String generateHistoryData(String warehouseName, String districtName) {
    if (warehouseName.length() > 10) {
      warehouseName = warehouseName.substring(0, 10);
    }
    if (districtName.length() > 10) {
      districtName = districtName.substring(0, 10);
    }
    return warehouseName + "    " + districtName;
  }

  @Override
  public void execute() throws TransactionException {
    transaction = manager.start();

    // Get and update warehouse
    Optional<Result> result = transaction.get(Warehouse.createGet(warehouseId));
    if (!result.isPresent()) {
      throw new IllegalStateException(
          String.format("Warehouse not found: warehouse: %d", warehouseId));
    }
    final String warehouseName =
        result.get().getValue(Warehouse.KEY_NAME).get().getAsString().get();
    final double warehouseYtd =
        result.get().getValue(Warehouse.KEY_YTD).get().getAsDouble() + paymentAmount;
    Warehouse warehouse = new Warehouse(warehouseId, warehouseYtd);
    transaction.put(warehouse.createPut());

    // Get and update district
    result = transaction.get(District.createGet(warehouseId, districtId));
    if (!result.isPresent()) {
      throw new IllegalStateException(
          String.format(
              "District not found: warehouse: %d, district: %d", warehouseId, districtId));
    }
    final String districtName = result.get().getValue(District.KEY_NAME).get().getAsString().get();
    final double districtYtd =
        result.get().getValue(District.KEY_YTD).get().getAsDouble() + paymentAmount;
    District district = new District(warehouseId, districtId, districtYtd);
    transaction.put(district.createPut());

    // Get and update customer
    if (byLastName) {
      if (config.useTableIndex()) {
        customerId =
            TpccUtil.getCustomerIdByTableIndex(
                transaction, warehouseId, districtId, customerLastName);
      } else {
        customerId =
            TpccUtil.getCustomerIdBySecondaryIndex(
                transaction, warehouseId, districtId, customerLastName);
      }
    }
    result =
        transaction.get(Customer.createGet(customerWarehouseId, customerDistrictId, customerId));
    if (!result.isPresent()) {
      throw new IllegalStateException(
          String.format(
              "Customer not found: warehouse: %d, district: %d, customer: %d",
              warehouseId, districtId, customerId));
    }
    final double balance =
        result.get().getValue(Customer.KEY_BALANCE).get().getAsDouble() + paymentAmount;
    final double ytdPayment =
        result.get().getValue(Customer.KEY_YTD_PAYMENT).get().getAsDouble() + paymentAmount;
    final int count = result.get().getValue(Customer.KEY_PAYMENT_CNT).get().getAsInt() + 1;
    final String credit = result.get().getValue(Customer.KEY_CREDIT).get().getAsString().get();
    String data = result.get().getValue(Customer.KEY_DATA).get().getAsString().get();
    if (credit.equals("BC")) {
      data =
          generateCustomerData(
              warehouseId,
              districtId,
              customerId,
              customerWarehouseId,
              customerDistrictId,
              paymentAmount,
              data);
    }
    Customer customer =
        new Customer(
            customerWarehouseId, customerDistrictId, customerId, balance, ytdPayment, count, data);
    transaction.put(customer.createPut());

    // Insert history
    final History history =
        new History(
            customerId,
            customerDistrictId,
            customerWarehouseId,
            districtId,
            warehouseId,
            date,
            paymentAmount,
            generateHistoryData(warehouseName, districtName));
    transaction.put(history.createPut());
  }

  @Override
  public void commit() throws TransactionException {
    transaction.commit();
  }

  @Override
  public void abort() throws TransactionException {
    transaction.abort();
  }
}
