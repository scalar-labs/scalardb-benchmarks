package com.scalar.db.benchmarks.tpcc.table;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Upsert;
import com.scalar.db.api.UpsertBuilder;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public abstract class TpccRecord {

  public static final String NAMESPACE = "tpcc";

  protected Map<String, Object> partitionKeyMap;
  protected Map<String, Object> clusteringKeyMap;
  protected Map<String, Object> valueMap;

  private void addToKeyBuilder(Key.Builder builder, String key, Object object) {
    if (object instanceof Integer) {
      builder.addInt(key, (Integer) object);
    } else if (object instanceof Double) {
      builder.addDouble(key, (Double) object);
    } else if (object instanceof String) {
      builder.addText(key, (String) object);
    } else if (object instanceof Date) {
      builder.addBigInt(key, ((Date) object).getTime());
    }
  }

  private Column<?> createSingleColumn(String key, Object object) {
    if (object instanceof Integer) {
      return IntColumn.of(key, (Integer) object);
    } else if (object instanceof Double) {
      return DoubleColumn.of(key, (Double) object);
    } else if (object instanceof String) {
      return TextColumn.of(key, (String) object);
    } else if (object instanceof Date) {
      return BigIntColumn.of(key, ((Date) object).getTime());
    }
    return null;
  }

  /** Creates a partition {@code Key}. */
  public Key createPartitionKey() {
    Key.Builder builder = Key.newBuilder();
    partitionKeyMap.forEach(
        (key, value) -> {
          if (value != null) {
            addToKeyBuilder(builder, key, value);
          }
        });
    return builder.build();
  }

  /** Creates a clustering {@code Key}. */
  public Key createClusteringKey() {
    Key.Builder builder = Key.newBuilder();
    clusteringKeyMap.forEach(
        (key, value) -> {
          if (value != null) {
            addToKeyBuilder(builder, key, value);
          }
        });
    return builder.build();
  }

  /**
   * Creates an {@code ArrayList} of {@code Column<?>}.
   *
   * @return an {@code ArrayList} of {@code Column<?>}
   */
  public ArrayList<Column<?>> createValues() {
    ArrayList<Column<?>> columns = new ArrayList<>();
    valueMap.forEach(
        (key, value) -> {
          if (value != null) {
            if (value instanceof Address) {
              columns.addAll(((Address) value).createValues());
            } else {
              Column<?> column = createSingleColumn(key, value);
              if (column != null) {
                columns.add(column);
              }
            }
          }
        });
    return columns;
  }

  protected static Get buildGet(String table, Key partitionKey) {
    return Get.newBuilder().namespace(NAMESPACE).table(table).partitionKey(partitionKey).build();
  }

  protected static Get buildGet(String table, Key partitionKey, Key clusteringKey) {
    return Get.newBuilder()
        .namespace(NAMESPACE)
        .table(table)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  protected static Put buildPut(String table, Key partitionKey) {
    return Put.newBuilder().namespace(NAMESPACE).table(table).partitionKey(partitionKey).build();
  }

  protected static Put buildPut(String table, Key partitionKey, Key clusteringKey) {
    return Put.newBuilder()
        .namespace(NAMESPACE)
        .table(table)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  protected static Delete buildDelete(String table, Key partitionKey, Key clusteringKey) {
    return Delete.newBuilder()
        .namespace(NAMESPACE)
        .table(table)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  protected static Scan buildScan(String table, Key partitionKey) {
    return Scan.newBuilder().namespace(NAMESPACE).table(table).partitionKey(partitionKey).build();
  }

  /** Converts a {@code Put} to an {@code Upsert} for use with {@code DistributedTransaction#upsert}. */
  public static Upsert toUpsert(Put put) {
    UpsertBuilder.Buildable builder =
        Upsert.newBuilder()
            .namespace(put.forNamespace().orElse(NAMESPACE))
            .table(put.forTable().orElseThrow(() -> new IllegalStateException("table is not set")))
            .partitionKey(put.getPartitionKey());
    put.getClusteringKey().ifPresent(builder::clusteringKey);
    for (Column<?> column : put.getColumns().values()) {
      builder = builder.value(column);
    }
    return builder.build();
  }

  /** Creates an {@code Upsert} equivalent to {@link #createPut()}. */
  public Upsert createUpsert() {
    return toUpsert(createPut());
  }

  /** Applies columns from {@link #createValues()} to a {@code Put}. */
  protected Put applyColumns(Put put) {
    PutBuilder.BuildableFromExisting builder = Put.newBuilder(put);
    for (Column<?> column : createValues()) {
      builder = builder.value(column);
    }
    return builder.build();
  }

  public abstract Put createPut();
}
