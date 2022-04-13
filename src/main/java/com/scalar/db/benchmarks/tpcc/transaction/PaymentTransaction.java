package com.scalar.db.benchmarks.tpcc.transaction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Customer;
import com.scalar.db.benchmarks.tpcc.TPCCTable.CustomerSecondary;
import com.scalar.db.benchmarks.tpcc.TPCCTable.District;
import com.scalar.db.benchmarks.tpcc.TPCCTable.History;
import com.scalar.db.benchmarks.tpcc.TPCCTable.Warehouse;
import com.scalar.db.benchmarks.tpcc.TPCCUtil;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;

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

  public void generate(int numWarehouse) {
    warehouseId = TPCCUtil.randomInt(1, numWarehouse);
    districtId = TPCCUtil.randomInt(1, Warehouse.DISTRICTS);
    paymentAmount = (float) (TPCCUtil.randomInt(100, 500000) / 100.0);
    date = new Date();

    int x = TPCCUtil.randomInt(1, 100);
    if (x <= 85) {
      // home warehouse
      customerWarehouseId = warehouseId;
      customerDistrictId = districtId;
    } else {
      // remote warehouse
      if (numWarehouse > 1) {
        do {
          customerWarehouseId = TPCCUtil.randomInt(1, numWarehouse);
        } while (customerWarehouseId == warehouseId);
      } else {
        customerWarehouseId = warehouseId;
      }
      customerDistrictId = TPCCUtil.randomInt(1, Warehouse.DISTRICTS);
    }

    int y = TPCCUtil.randomInt(1, 100);
    if (y <= 60) {
      // by last name
      byLastName = true;
      customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun();
    } else {
      // by customer id
      byLastName = false;
      customerId = TPCCUtil.getCustomerId();
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

  public void execute(DistributedTransactionManager manager) throws TransactionException {
    DistributedTransaction tx = manager.start();

    try {
      Get get;
      Put put;
      Optional<Result> result;

      // Get and update warehouse
      Key warehouseKey = Warehouse.createPartitionKey(warehouseId);
      get = new Get(warehouseKey);
      result = tx.get(get.forTable(Warehouse.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("Warehouse not found");
      }
      String warehouseName = result.get().getValue(Warehouse.KEY_NAME).get().getAsString().get();
      double warehouseYTD =
          result.get().getValue(Warehouse.KEY_YTD).get().getAsDouble() + paymentAmount;
      put = new Put(warehouseKey).withValue(Warehouse.KEY_YTD, warehouseYTD);
      tx.put(put.forTable(Warehouse.TABLE_NAME));

      // Get and update district
      Key districtKey = District.createPartitionKey(warehouseId, districtId);
      get = new Get(districtKey);
      result = tx.get(get.forTable(District.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("District not found");
      }
      String districtName = result.get().getValue(District.KEY_NAME).get().getAsString().get();
      double districtYTD =
          result.get().getValue(District.KEY_YTD).get().getAsDouble() + paymentAmount;
      put = new Put(districtKey).withValue(District.KEY_YTD, districtYTD);
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
      Key customerKey = Customer.createPartitionKey(warehouseId, districtId, customerId);
      get = new Get(customerKey);
      result = tx.get(get.forTable(Customer.TABLE_NAME));
      if (!result.isPresent()) {
        throw new TransactionException("Customer not found");
      }
      double balance =
          result.get().getValue(Customer.KEY_BALANCE).get().getAsDouble() + paymentAmount;
      double ytdPayment =
          result.get().getValue(Customer.KEY_YTD_PAYMENT).get().getAsDouble() + paymentAmount;
      int count = result.get().getValue(Customer.KEY_PAYMENT_CNT).get().getAsInt() + 1;
      String credit = result.get().getValue(Customer.KEY_CREDIT).get().getAsString().get();
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
      History history = new History(customerId, customerDistrictId, customerWarehouseId, districtId,
          warehouseId, date, paymentAmount, generateHistoryData(warehouseName, districtName));
      Key historyKey = history.createPartitionKey();
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
