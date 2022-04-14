package com.scalar.db.benchmarks.tpcc.transaction;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.benchmarks.tpcc.TpccTable.Customer;
import com.scalar.db.benchmarks.tpcc.TpccTable.CustomerSecondary;
import com.scalar.db.benchmarks.tpcc.TpccTable.District;
import com.scalar.db.benchmarks.tpcc.TpccTable.History;
import com.scalar.db.benchmarks.tpcc.TpccTable.Warehouse;
import com.scalar.db.benchmarks.tpcc.TpccUtil;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.ArrayList;
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
      final Key warehouseKey = Warehouse.createPartitionKey(warehouseId);
      Get get = new Get(warehouseKey);
      Optional<Result> result = tx.get(get.forTable(Warehouse.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("Warehouse not found");
      }
      final String warehouseName =
          result.get().getValue(Warehouse.KEY_NAME).get().getAsString().get();
      final double warehouseYtd =
          result.get().getValue(Warehouse.KEY_YTD).get().getAsDouble() + paymentAmount;
      Put put = new Put(warehouseKey).withValue(Warehouse.KEY_YTD, warehouseYtd);
      tx.put(put.forTable(Warehouse.TABLE_NAME));

      // Get and update district
      final Key districtKey = District.createPartitionKey(warehouseId, districtId);
      get = new Get(districtKey);
      result = tx.get(get.forTable(District.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("District not found");
      }
      final String districtName =
          result.get().getValue(District.KEY_NAME).get().getAsString().get();
      final double districtYtd =
          result.get().getValue(District.KEY_YTD).get().getAsDouble() + paymentAmount;
      put = new Put(districtKey).withValue(District.KEY_YTD, districtYtd);
      tx.put(put.forTable(District.TABLE_NAME));

      // Get and update customer
      if (byLastName) {
        Key key = CustomerSecondary.createPartitionKey(warehouseId, districtId, customerLastName);
        Scan scan = new Scan(key);
        List<Result> results = tx.scan(scan.forTable(CustomerSecondary.TABLE_NAME));
        int offset = (results.size() + 1) / 2 - 1; // locate midpoint customer
        customerId =
            results.get(offset).getValue(CustomerSecondary.KEY_CUSTOMER_ID).get().getAsInt();
      }
      final Key customerKey = Customer.createPartitionKey(warehouseId, districtId, customerId);
      get = new Get(customerKey);
      result = tx.get(get.forTable(Customer.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("Customer not found");
      }
      final double balance =
          result.get().getValue(Customer.KEY_BALANCE).get().getAsDouble() + paymentAmount;
      final double ytdPayment =
          result.get().getValue(Customer.KEY_YTD_PAYMENT).get().getAsDouble() + paymentAmount;
      final int count = result.get().getValue(Customer.KEY_PAYMENT_CNT).get().getAsInt() + 1;
      final String credit = result.get().getValue(Customer.KEY_CREDIT).get().getAsString().get();
      ArrayList<Value<?>> customerValues = new ArrayList<Value<?>>();
      customerValues.add(new DoubleValue(Customer.KEY_BALANCE, balance));
      customerValues.add(new DoubleValue(Customer.KEY_YTD_PAYMENT, ytdPayment));
      customerValues.add(new IntValue(Customer.KEY_PAYMENT_CNT, count));
      if (credit.equals("BC")) {
        String oldData = result.get().getValue(Customer.KEY_DATA).get().getAsString().get();
        String newData = generateCustomerData(warehouseId, districtId, customerId,
            customerWarehouseId, customerDistrictId, paymentAmount, oldData);
        customerValues.add(new TextValue(Customer.KEY_DATA, newData));
      }
      put = new Put(customerKey).withValues(customerValues);
      tx.put(put.forTable(Customer.TABLE_NAME));

      // Insert history
      final History history =
          new History(customerId, customerDistrictId, customerWarehouseId, districtId, warehouseId,
              date, paymentAmount, generateHistoryData(warehouseName, districtName));
      final Key historyKey = history.createPartitionKey();
      ArrayList<Value<?>> historyValues = history.createValues();
      put = new Put(historyKey).withValues(historyValues);
      tx.put(put.forTable(History.TABLE_NAME));

      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }
}
