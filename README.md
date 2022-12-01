# ScalarDB Benchmarks

This repository contains benchmark programs for ScalarDB. Currently, only TPC-C is available for the workloads.

## Prerequisites

- Java (OpenJDK 8 or higher)
- Gradle

## Usage

### Set up an environment

This benchmark requires the followings:
- A client to execute this benchmark
- A target database (See [here](https://github.com/scalar-labs/scalardb/blob/master/docs/scalardb-supported-databases.md) for supported databases)

### Build

```console
./gradlew installDist
```

### Load 

Before loading initial data, the tables must be defined using [ScalarDB Schema Loader](https://github.com/scalar-labs/scalardb/blob/master/docs/schema-loader.md). Get the latest schema laoder [here](https://github.com/scalar-labs/scalardb/releases) and execute it with `tpcc-schema.json`.

For setting ScalarDB properties, see also the related documents [here](https://github.com/scalar-labs/scalardb#docs).


```console
java -jar scalardb-schema-loader-<version>.jar --config /path/to/scalardb.properties -f tpcc-schema.json
```

Then, load initial data with your preferred scale factor.

```console
cd build/install/scalardb-benchmarks
./bin/tpcc-loader --config /path/to/scalardb.properties --num-warehouse 1 --num-threads 32
```

### Run

```console
./bin/tpcc-bench --config /path/to/scalardb.properties --num-warehouse 1 --num-threads 4
```

If successfully done, you can get throughput and average latency.

## Optional Parameters

For initial loading, you can specify the following options.

| name                  | description | default |
|:----------------------|:------------|:--------|
| `--num-threads`       | Number of thread for loading. | 1 |
| `--num-warehouse`     | Number of warehouse (scale factor) for loading. | 1 |
| `--start-warehouse`   | Start ID of loading warehouse. This option can be useful with `--skip-item-load` when loading large scale data with multiple clients or adding additional warehouses. | 1 |
| `--skip-item-load`    | Whether or not to skip loading item table | false |
| `--use-table-index`   | Whether or not to use a generic table-based secondary index instead of ScalarDB's secondary index. | false |

For benchmarking, you can specify the following options.

| name                  | description | default |
|:----------------------|:------------|:--------|
| `--num-threads`       | Number of thread for benchmarking. | 1 |
| `--num-warehouse`     | Number of warehouse (scale factor) for benchmarking. | 1 |
| `--duration`          | Duration of benchmark (in seconds). | 200 |
| `--ramp-up-time`      | Duration of ramp up time before benchmark (in seconds). | 30 |
| `--use-table-index`   | Whether or not to use a generic table-based secondary index instead of ScalarDB's secondary index. Data must be loaded using the same option. | false |
| `--rate-new-order`    | Percentage of new-order transaction.     | 45 |
| `--rate-payment`      | Percentage of payment transaction.       | 43 |
| `--rate-order-status` | Percentage of order-status transaction.  | 4  |
| `--rate-delivery`     | Percentage of delivery transaction.      | 4  |
| `--rate-stock-level`  | Percentage of stock-level transaction.   | 4  |
| `--np-only`           | Run benchmark with only new-order and payment transactions (50% each). | false |
