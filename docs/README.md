# ScalarDB Benchmarks

This repository contains benchmark programs for ScalarDB.

## Available workloads

- TPC-C
- YCSB (Workload A, C, and F)
- Multi-storage YCSB (Workload C and F)
  - A YCSB-variant benchmark using a multi-storage environment of ScalarDB.
  - Workers in multi-storage YCSB execute the same number of read/write operations in two namespaces (`ycsb_primary` and `ycsb_secondary`).

## Prerequisites

- Java (OpenJDK 8 or higher)
- Gradle
- Kelpie

The benchmark uses Kelpie, which is a simple yet general framework for performing end-to-end testing such as system benchmarking and verification. Get the latest version of Kelpie from [here](https://github.com/scalar-labs/kelpie) and unzip the archive.

## Usage

### Set up an environment

This benchmark requires the followings:
- A client to execute this benchmark
- A target database (See [here](https://github.com/scalar-labs/scalardb/blob/master/docs/scalardb-supported-databases.md) for supported databases)

### Build

```console
./gradlew shadowJar
```

### Create tables

Before loading initial data, the tables must be defined using [ScalarDB Schema Loader](https://github.com/scalar-labs/scalardb/blob/master/docs/schema-loader.md). Get the latest schema loader [here](https://github.com/scalar-labs/scalardb/releases) and execute it with the workload-specific schema file. For setting ScalarDB properties, see also the related documents [here](https://github.com/scalar-labs/scalardb#docs).

For example, execute the following command with `tpcc-schema.json` to create tables for TPC-C benchmark.
For YCSB and multi-storage YCSB, use `ycsb-schema.json` and `ycsb-multi-storage-schema.json`, respectively.

```console
java -jar scalardb-schema-loader-<version>.jar --config /path/to/scalardb.properties -f tpcc-schema.json --coordinator
```

### Load and run

1. Prepare a configuration file
    - A configuration file requires at least the locations of workload modules to run and the database configuration. The following example shows the case for running TPC-C benchmark. The database configuration should be matched with the benchmark environment set up above. You can use the ScalarDB property file instead of specifying each configuration item. If the `config_file` is specified, all other configuration items will be ignored.
      ```
      [modules]
      [modules.preprocessor]
      name = "com.scalar.db.benchmarks.tpcc.TpccLoader"
      path = "./build/libs/scalardb-benchmarks-all.jar"
      [modules.processor]
      name = "com.scalar.db.benchmarks.tpcc.TpccBench"
      path = "./build/libs/scalardb-benchmarks-all.jar"
      [modules.postprocessor]
      name = "com.scalar.db.benchmarks.tpcc.TpccReporter"
      path = "./build/libs/scalardb-benchmarks-all.jar"
 
      [database_config]
      contact_points = "localhost"
      contact_port = 9042
      username = "cassandra"
      password = "cassandra"
      storage = "cassandra"
      #config_file = "/path/to/scalardb.properties"
      ```
    - You can define static parameters to pass to modules in the file. For details, see the sample configuration files below and available parameters in [the following section](#common-parameters).
      - TPC-C: `tpcc-benchmark-config.toml`
      - YCSB: `ycsb-benchmark-config.toml`
      - Multi-storage YCSB: `ycsb-multi-storage-benchmark-config.toml`
2. Run a benchmark
   ```
   ${kelpie}/bin/kelpie --config your_config.toml
   ```
    - `${kelpie}` is a Kelpie directory, which is extracted from the archive you downloaded [above](#prerequisites).
    - There are other options such as `--only-pre` (i.e., loading data) and `--only-process` (i.e., running benchmark), which run only the specified process. `--except-pre` and `--except-process` run a job without the specified process.

## Common parameters

| name           | description                                             | default |
|:---------------|:--------------------------------------------------------|:--------|
| `concurrency`  | Number of threads for benchmarking.                     | 1       |
| `run_for_sec`  | Duration of benchmark (in seconds).                     | 60      |
| `ramp_for_sec` | Duration of ramp up time before benchmark (in seconds). | 0       |

## Workload-specific parameters

### TPC-C

| name                   | description                                                                                                                                                           | default |
|:-----------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------|
| `num_warehouses`       | Number of warehouses (scale factor) for benchmarking.                                                                                                                 | 1       |
| `load_concurrency`     | Number of threads for loading.                                                                                                                                        | 1       |
| `load_start_warehouse` | Start ID of loading warehouse. This option can be useful with `--skip-item-load` when loading large scale data with multiple clients or adding additional warehouses. | 1       |
| `load_end_warehouse`   | End ID of loading warehouse. You can use either `--num-warehouses` or `--end-warehouse` to specify the number of loading warehouses.                                  | 1       |
| `skip_item_load`       | Whether or not to skip loading item table                                                                                                                             | false   |
| `use_table_index`      | Whether or not to use a generic table-based secondary index instead of ScalarDB's secondary index.                                                                    | false   |
| `np_only`              | Run benchmark with only new-order and payment transactions (50% each).                                                                                                | false   |
| `rate_new_order`       | Percentage of new-order transaction. Specify all the rate parameters to use your own mix.                                                                             | N/A     |
| `rate_payment`         | Percentage of payment transaction. Specify all the rate parameters to use your own mix.                                                                               | N/A     |
| `rate_order_status`    | Percentage of order-status transaction. Specify all the rate parameters to use your own mix.                                                                          | N/A     |
| `rate_delivery`        | Percentage of delivery transaction. Specify all the rate parameters to use your own mix.                                                                              | N/A     |
| `rate_stock_level`     | Percentage of stock-level transaction. Specify all the rate parameters to use your own mix.                                                                           | N/A     |
| `backoff`              | Sleep time in milliseconds inserted after a transaction is aborted due to a conflict.                                                                                 | 0       |

### YCSB and multi-storage YCSB

| name                    | description                                                                       | default                                   |
|:------------------------|:----------------------------------------------------------------------------------|:------------------------------------------|
| `load_concurrency`      | Number of threads for loading.                                                    | 1                                         |
| `load_batch_size`       | Number of put records in a single loading transaction.                            | 1                                         |
| `load_overwrite`        | Whether or not to overwrite when loading records.                                 | false                                     |
| `ops_per_tx`            | Number of operations in a single transaction.                                     | 2 (Workload A and C) <br> 1 (Workload F)  |
| `record_count`          | Number of records in the target table.                                            | 1000                                      |
| `use_read_modify_write` | Whether or not to use read-modify-writes instead of blind writes in Workload A.   | false[^rmw]                               |

[^rmw]: The default value is `false` for `use_read_modify_write` since Workload A does not assume the transaction reads the original record first.
However, you need to set `true` if you use the consensus commit for the transaction manager because ScalarDB does not allow a blind write for the existing record.
