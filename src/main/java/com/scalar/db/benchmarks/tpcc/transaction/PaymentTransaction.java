package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.benchmarks.tpcc.table.Customer;
import com.scalar.db.benchmarks.tpcc.table.CustomerSecondary;
import com.scalar.db.benchmarks.tpcc.table.District;
import com.scalar.db.benchmarks.tpcc.table.History;
import com.scalar.db.benchmarks.tpcc.table.Warehouse;
import com.scalar.db.exception.transaction.TransactionException;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class PaymentTransaction {
  int warehouseId;
  int districtId;
  int customerId;
  int customerWarehouseId;
  int customerDistrictId;
  String customerLastName;
  boolean byLastName;
  float paymentAmount;
  Date date;

  /**
   * Generates arguments for the payment transaction.
   * 
   * @param numWarehouse a number of warehouse
   */
  public void generate(int numWarehouse) {
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

  private String generateCustomerData(int warehouseId, int districtId, int customerId,
      int customerWarehouseId, int customerDistrictId, double amount, String oldData) {
    String data = customerId + " " + customerDistrictId + " " + customerWarehouseId + " "
        + districtId + " " + warehouseId + " " + String.format("%7.2f", amount) + " | " + oldData;
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

  /**
   * Executes the payment transaction.
   * 
   * @param manager a {@code DistributedTransactionManager} object
   */
  public void execute(DistributedTransactionManager manager) throws TransactionException {
    DistributedTransaction tx = manager.start();

    try {
      // Get and update warehouse
      Optional<Result> result = tx.get(Warehouse.createGet(warehouseId));
      if (!result.isPresent()) {
        throw new TransactionException("Warehouse not found");
      }
      final String warehouseName =
          result.get().getValue(Warehouse.KEY_NAME).get().getAsString().get();
      final double warehouseYtd =
          result.get().getValue(Warehouse.KEY_YTD).get().getAsDouble() + paymentAmount;
      Warehouse warehouse = new Warehouse(warehouseId, warehouseYtd);
      tx.put(warehouse.createPut());

      // Get and update district
      result = tx.get(District.createGet(warehouseId, districtId));
      if (!result.isPresent()) {
        throw new TransactionException("District not found");
      }
      final String districtName =
          result.get().getValue(District.KEY_NAME).get().getAsString().get();
      final double districtYtd =
          result.get().getValue(District.KEY_YTD).get().getAsDouble() + paymentAmount;
      District district = new District(warehouseId, districtId, districtYtd);
      tx.put(district.createPut());

      // Get and update customer
      if (byLastName) {
        List<Result> results = tx.scan(
            CustomerSecondary.createScan(warehouseId, districtId, customerLastName));
        int offset = (results.size() + 1) / 2 - 1; // locate midpoint customer
        customerId =
            results.get(offset).getValue(CustomerSecondary.KEY_CUSTOMER_ID).get().getAsInt();
      }
      result = tx.get(Customer.createGet(warehouseId, districtId, customerId));
      if (!result.isPresent()) {
        throw new TransactionException("Customer not found");
      }
      final double balance =
          result.get().getValue(Customer.KEY_BALANCE).get().getAsDouble() + paymentAmount;
      final double ytdPayment =
          result.get().getValue(Customer.KEY_YTD_PAYMENT).get().getAsDouble() + paymentAmount;
      final int count = result.get().getValue(Customer.KEY_PAYMENT_CNT).get().getAsInt() + 1;
      final String credit = result.get().getValue(Customer.KEY_CREDIT).get().getAsString().get();
      String data = result.get().getValue(Customer.KEY_DATA).get().getAsString().get();
      if (credit.equals("BC")) {
        data = generateCustomerData(warehouseId, districtId, customerId,
            customerWarehouseId, customerDistrictId, paymentAmount, data);
      }
      Customer customer = new Customer(warehouseId, districtId, customerId,
          balance, ytdPayment, count, data);
      tx.put(customer.createPut());

      // Insert history
      final History history =
          new History(customerId, customerDistrictId, customerWarehouseId, districtId, warehouseId,
              date, paymentAmount, generateHistoryData(warehouseName, districtName));
      tx.put(history.createPut());

      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }
}
