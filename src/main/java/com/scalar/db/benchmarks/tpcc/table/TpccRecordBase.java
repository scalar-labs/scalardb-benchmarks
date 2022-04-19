package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public abstract class TpccRecordBase implements TpccRecord {
  public static final String NAMESPACE = "tpcc";

  protected Map<String,Object> partitionKeyMap;
  protected Map<String,Object> clusteringKeyMap;
  protected Map<String,Object> valueMap;

  protected void insert(DistributedTransactionManager manager, String table, Key partitionKey,
      ArrayList<Value<?>> values) throws TransactionException {
    DistributedTransaction tx = manager.start();
    tx.with(NAMESPACE, table);
    try {
      tx.put(new Put(partitionKey).withValues(values));
      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  protected void insert(DistributedTransactionManager manager, String table, Key partitionKey,
      Key clusteringKey, ArrayList<Value<?>> values) throws TransactionException {
    DistributedTransaction tx = manager.start();
    tx.with(NAMESPACE, table);
    try {
      tx.put(new Put(partitionKey, clusteringKey).withValues(values));
      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  protected void insert(DistributedTransactionManager manager, String table, Key partitionKey,
      Key clusteringKey) throws TransactionException {
    DistributedTransaction tx = manager.start();
    tx.with(NAMESPACE, table);
    try {
      tx.put(new Put(partitionKey, clusteringKey));
      tx.commit();
    } catch (Exception e) {
      tx.abort();
      throw e;
    }
  }

  private Value<?> createSingleValue(String key, Object object) {
    if (object instanceof Integer) {
      return new IntValue(key, (Integer)object);
    } else if (object instanceof Double) {
      return new DoubleValue(key, (Double)object);
    } else if (object instanceof String) {
      return new TextValue(key, (String)object);
    } else if (object instanceof Date) {
      return new BigIntValue(key, ((Date)object).getTime());
    }
    return null;
  }

  /**
   * Creates a partition {@code Key}.
   */
  public Key createPartitionKey() {
    ArrayList<Value<?>> values = new ArrayList<Value<?>>();
    partitionKeyMap.forEach((key, value) -> {
      if (value != null) {
        values.add(createSingleValue(key, value));
      }
    });
    return new Key(values);
  }

  /**
   * Creates a clustering {@code Key}.
   */
  public Key createClusteringKey() {
    ArrayList<Value<?>> values = new ArrayList<Value<?>>();
    clusteringKeyMap.forEach((key, value) -> {
      if (value != null) {
        values.add(createSingleValue(key, value));
      }
    });
    return new Key(values);
  }

  /**
   * Creates an {@code ArrayList} of {@code Value<?>}.
   * 
   * @return an {@code ArrayList} of {@code Value<?>}
   */
  public ArrayList<Value<?>> createValues() {
    ArrayList<Value<?>> values = new ArrayList<Value<?>>();
    valueMap.forEach((key, value) -> {
      if (value != null) {
        if (value instanceof Address) {
          values.addAll(((Address)value).createValues());
        } else {
          values.add(createSingleValue(key, value));
        }
      }
    });
    return values;
  }
}
