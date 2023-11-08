# ScalarDB Benchmarks

This tutorial describes how to set up and run benchmarking tools for ScalarDB.

## Available workloads

- TPC-C
- YCSB (Workloads A, C, and F)
- Multi-storage YCSB (Workloads C and F)
  - This YCSB variant is for a multi-storage environment that uses ScalarDB.
  - Workers in a multi-storage YCSB execute the same number of read and write operations in two namespaces: `ycsb_primary` and `ycsb_secondary`.

## Prerequisites

- One of the following Java Development Kits (JDKs):
  - [Oracle JDK](https://www.oracle.com/java/technologies/downloads/) LTS version 8
  - [OpenJDK](https://openjdk.org/install/) LTS version 8
- Gradle
- [Kelpie](https://github.com/scalar-labs/kelpie)
  - Kelpie is a framework for performing end-to-end testing, such as system benchmarking and verification. Get the latest version from [Kelpie Releases](https://github.com/scalar-labs/kelpie), and unzip the archive file.

{% capture notice--info %}
**Note**

Currently, only JDK 8 can be used when running the benchmarking tools.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

## Set up your environment

The benchmarking tools require the following:

- A client to run the benchmarking tools
- A target database
  - For a list of databases that ScalarDB supports, see [Supported Databases](https://github.com/scalar-labs/scalardb/blob/master/docs/scalardb-supported-databases.md).

## Set up the benchmarking tools

The following sections describe how to set up the benchmarking tools.

### Clone the ScalarDB benchmarks repository

Open **Terminal**, then clone the ScalarDB benchmarks repository by running the following command:

```console
$ git clone https://github.com/scalar-labs/scalardb-benchmarks
```

Then, go to the directory that contains the benchmarking files by running the following command:

```console
$ cd scalardb-benchmarks
```

### Build the tools

To build the benchmarking tools, run the following command:

```console
$ ./gradlew shadowJar
```

### Create tables

Before loading the initial data, the tables must be defined by using the [ScalarDB Schema Loader](https://github.com/scalar-labs/scalardb/blob/master/docs/schema-loader.md). To apply the schema, go to the [ScalarDB Releases](https://github.com/scalar-labs/scalardb/releases) page and download the ScalarDB Schema Loader that matches the version of ScalarDB that you are using to the `scalardb-benchmarks` root folder.

In addition, you need a properties file that contains database configurations for ScalarDB. For details about configuring the ScalarDB properties file, see [ScalarDB Configurations](https://github.com/scalar-labs/scalardb/blob/master/docs/configurations.md).

After applying the schema and configuring the properties file, select a benchmark and follow the instructions to create the tables.

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'TPC-C_1', 'tabset-1')" id="defaultOpen-1">TPC-C</button>
  <button class="tablinks" onclick="openTab(event, 'YCSB_1', 'tabset-1')">YCSB</button>
  <button class="tablinks" onclick="openTab(event, 'multi-storage_YCSB_1', 'tabset-1')">Multi-storage YCSB</button>
</div>

<div id="TPC-C_1" class="tabcontent" markdown="1">

To create tables for TPC-C benchmarking ([`tpcc-schema.json`](https://github.com/scalar-labs/scalardb-benchmarks/blob/master/tpcc-schema.json)), run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config /<PATH_TO>/scalardb.properties -f tpcc-schema.json --coordinator
```
</div>
<div id="YCSB_1" class="tabcontent" markdown="1">

To create tables for YCSB benchmarking ([`ycsb-schema.json`](https://github.com/scalar-labs/scalardb-benchmarks/blob/master/ycsb-schema.json)), run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config /<PATH_TO>/scalardb.properties -f ycsb-schema.json --coordinator
```
</div>
<div id="multi-storage_YCSB_1" class="tabcontent" markdown="1">

To create tables for multi-storage YCSB benchmarking ([`ycsb-multi-storage-schema.json`](https://github.com/scalar-labs/scalardb-benchmarks/blob/master/ycsb-multi-storage-schema.json)), run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config /<PATH_TO>/scalardb.properties -f ycsb-multi-storage-schema.json --coordinator
```
</div>
</div>

### Prepare a benchmarking configuration file

To run a benchmark, you must first prepare a benchmarking configuration file. The configuration file requires at least the locations of the workload modules to run and the database configuration. 

The following is an example configuration for running the TPC-C benchmark. The configurations under `database_config` should match the [benchmarking environment that you previously set up](#set-up-your-environment).

{% capture notice--info %}
**Note**

Alternatively, instead of specifying each database configuration item in the `.toml` file, you can use the ScalarDB properties file. If `config_file` is specified (commented out below), all other configurations under `database_config` will be ignored.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

```toml
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
#config_file = "/<PATH_TO>/scalardb.properties"
```

You can define static parameters to pass to modules in the configuration file. For details, see the sample configuration files below and available parameters in [Common parameters](#common-parameters):

- **TPC-C:** [`tpcc-benchmark-config.toml`](https://github.com/scalar-labs/scalardb-benchmarks/blob/master/tpcc-benchmark-config.toml)
- **YCSB:** [`ycsb-benchmark-config.toml`](https://github.com/scalar-labs/scalardb-benchmarks/blob/master/ycsb-benchmark-config.toml)
- **Multi-storage YCSB:** [`ycsb-multi-storage-benchmark-config.toml`](https://github.com/scalar-labs/scalardb-benchmarks/blob/master/ycsb-multi-storage-benchmark-config.toml)

## Run a benchmark

Select a benchmark, and follow the instructions to run the benchmark.

<div id="tabset-2">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'TPC-C_2', 'tabset-2')" id="defaultOpen-2">TPC-C</button>
  <button class="tablinks" onclick="openTab(event, 'YCSB_2', 'tabset-2')">YCSB</button>
  <button class="tablinks" onclick="openTab(event, 'multi-storage_YCSB_2', 'tabset-2')">Multi-storage YCSB</button>
</div>

<div id="TPC-C_2" class="tabcontent" markdown="1">

To run the TPC-C benchmark, run the following command, replacing `<PATH_TO_KELPIE>` with the path to the Kelpie directory:

```console
$ /<PATH_TO_KELPIE>/bin/kelpie --config tpcc-benchmark-config.toml
```
</div>
<div id="YCSB_2" class="tabcontent" markdown="1">

To run the YCSB benchmark, run the following command, replacing `<PATH_TO_KELPIE>` with the path to the Kelpie directory:

```console
$ /<PATH_TO_KELPIE>/bin/kelpie --config ycsb-benchmark-config.toml
```
</div>
<div id="multi-storage_YCSB_2" class="tabcontent" markdown="1">

To run the multi-storage YCSB benchmark, run the following command, replacing `<PATH_TO_KELPIE>` with the path to the Kelpie directory:

```console
$ /<PATH_TO_KELPIE>/bin/kelpie --config ycsb-multi-storage-benchmark-config.toml
```
</div>
</div>

In addition, the following options are available:

- `--only-pre`. Only loads the data.
- `--only-process`. Only runs the benchmark.
- `--except-pre` Runs a job without loading the data.
- `--except-process`. Runs a job without running the benchmark.

## Common parameters

| Name           | Description                                             | Default   |
|:---------------|:--------------------------------------------------------|:----------|
| `concurrency`  | Number of threads for benchmarking.                     | `1`       |
| `run_for_sec`  | Duration of benchmark (in seconds).                     | `60`      |
| `ramp_for_sec` | Duration of ramp-up time before benchmark (in seconds). | `0`       |

## Workload-specific parameters

Select a workload to see its available parameters.

<div id="tabset-3">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'TPC-C_3', 'tabset-3')" id="defaultOpen-3">TPC-C</button>
  <button class="tablinks" onclick="openTab(event, 'YCSB_and_multi-storage_YCSB', 'tabset-3')">YCSB and multi-storage YCSB</button>
</div>

<div id="TPC-C_3" class="tabcontent" markdown="1">

| Name                   | Description                                                                                                                                                           | Default   |
|:-----------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------|
| `num_warehouses`       | Number of warehouses (scale factor) for benchmarking.                                                                                                                 | `1`       |
| `load_concurrency`     | Number of threads for loading.                                                                                                                                        | `1`       |
| `load_start_warehouse` | Start ID of loading warehouse. This option can be useful with `--skip-item-load` when loading large-scale data with multiple clients or adding additional warehouses. | `1`       |
| `load_end_warehouse`   | End ID of loading warehouse. You can use either `--num-warehouses` or `--end-warehouse` to specify the number of loading warehouses.                                  | `1`       |
| `skip_item_load`       | Whether or not to skip loading item table.                                                                                                                            | `false`   |
| `use_table_index`      | Whether or not to use a generic table-based secondary index instead of ScalarDB's secondary index.                                                                    | `false`   |
| `np_only`              | Run benchmark with only new-order and payment transactions (50% each).                                                                                                | `false`   |
| `rate_new_order`       | Percentage of new-order transactions. Specify all the rate parameters to use your own mix.                                                                            | `N/A`     |
| `rate_payment`         | Percentage of payment transactions. Specify all the rate parameters to use your own mix.                                                                              | `N/A`     |
| `rate_order_status`    | Percentage of order-status transactions. Specify all the rate parameters to use your own mix.                                                                         | `N/A`     |
| `rate_delivery`        | Percentage of delivery transactions. Specify all the rate parameters to use your own mix.                                                                             | `N/A`     |
| `rate_stock_level`     | Percentage of stock-level transactions. Specify all the rate parameters to use your own mix.                                                                          | `N/A`     |
| `backoff`              | Sleep time in milliseconds inserted after a transaction is aborted due to a conflict.                                                                                 | `0`       |

</div>
<div id="YCSB_and_multi-storage_YCSB" class="tabcontent" markdown="1">

| Name                    | Description                                                                       | Default                                       |
|:------------------------|:----------------------------------------------------------------------------------|:----------------------------------------------|
| `load_concurrency`      | Number of threads for loading.                                                    | `1`                                           |
| `load_batch_size`       | Number of put records in a single loading transaction.                            | `1`                                           |
| `load_overwrite`        | Whether or not to overwrite when loading records.                                 | `false`                                       |
| `ops_per_tx`            | Number of operations in a single transaction.                                     | `2` (Workload A and C) <br> `1` (Workload F)  |
| `record_count`          | Number of records in the target table.                                            | `1000`                                        |
| `use_read_modify_write` | Whether or not to use read-modify-writes instead of blind writes in Workload A.   | `false`[^rmw]                                 |

[^rmw]: The default value is `false` for `use_read_modify_write` since Workload A doesn't assume that the transaction reads the original record first. However, if you're using Consensus Commit as the transaction manager, you must set `use_read_modify_write` to `true`. This is because ScalarDB doesn't allow a blind write for an existing record.
</div>
</div>
