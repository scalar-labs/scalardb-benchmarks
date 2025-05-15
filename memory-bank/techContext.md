# ScalarDB ベンチマークツール: 技術コンテキスト

## 使用技術

### 言語とランタイム
- **Java**: JDK 8（必須要件）
- **Gradle**: ビルドツールとして使用

### 主要フレームワーク
- **ScalarDB**: 評価対象のデータベースミドルウェア
- **Kelpie**: ベンチマークの実行フレームワーク
  - プリプロセッサ、プロセッサ、ポストプロセッサの枠組みを提供
  - 設定ファイル（TOML）によるパラメータ指定をサポート

### 利用ライブラリ
- **ScalarDB Java API**: トランザクション処理のため
- **ScalarDB Admin API**: ユーザー作成・権限管理のため（マルチユーザーモード）
- **resilience4j-retry**: リトライ処理のため
- **javax.json**: JSON処理のため
- **guava**: ユーティリティ機能のため

## 開発環境

### 必須要件
- JDK 8（Oracle JDK または OpenJDK）
- Gradle
- Kelpie フレームワーク

### 推奨ツール
- Java対応IDE（IntelliJ IDEA, Eclipse, VSCode など）
- Git（バージョン管理）

## 技術的制約

### JDKバージョン
- JDK 8のみサポート（現時点ではより新しいバージョンは動作保証されていない）

### データベース制約
- ScalarDBでサポートされているデータベースのみテスト可能
  - Cassandra
  - MongoDB
  - MySQL
  - PostgreSQL
  - Oracle
  - SQL Server
  - Cosmos DB
  - DynamoDB
  - その他ScalarDBがサポートするストレージ

### Kelpie依存性
- Kelpieフレームワークのインターフェースに準拠する必要がある
- モジュール間のデータやり取りはKelpieの仕組みに則る

### マルチユーザーモードの制約
- ユーザー作成と権限管理にはScalarDB Cluster環境が必要
- ScalarDB Communityでは管理者権限で実行される
- ユーザー数はレコード数以下である必要がある

## 依存関係

### 主要な外部依存性
```gradle
dependencies {
  implementation 'com.scalar-labs:scalardb:[バージョン]'
  implementation 'com.scalar-labs:kelpie-core:[バージョン]'
  implementation 'io.github.resilience4j:resilience4j-retry:[バージョン]'
  implementation 'org.slf4j:slf4j-api:[バージョン]'
  implementation 'javax.json:javax.json-api:[バージョン]'
  implementation 'com.google.guava:guava:[バージョン]'
}
```

### データベース依存性
- テスト対象のデータベースに応じた追加依存性が必要
  - 例：Cassandra用のドライバ、JDBC接続用のドライバなど

## ツール使用パターン

### ビルドとパッケージング

```bash
# ビルド（shadowJarで依存関係を含めた単一JARを生成）
./gradlew shadowJar
```

### スキーマ作成

```bash
# ScalarDB Schema Loaderを使用
java -jar scalardb-schema-loader-<VERSION>.jar --config <PATH_TO_SCALARDB_PROPERTIES_FILE> -f <SCHEMA_FILE> --coordinator
```

### ベンチマーク実行

```bash
# Kelpieを使ってベンチマーク実行
/<PATH_TO_KELPIE>/bin/kelpie --config <CONFIG_FILE>

# オプション
# データロードのみ
/<PATH_TO_KELPIE>/bin/kelpie --config <CONFIG_FILE> --only-pre

# ベンチマーク実行のみ
/<PATH_TO_KELPIE>/bin/kelpie --config <CONFIG_FILE> --only-process

# データロードなしでベンチマーク実行
/<PATH_TO_KELPIE>/bin/kelpie --config <CONFIG_FILE> --except-pre

# ベンチマーク実行なしでデータロードと結果処理
/<PATH_TO_KELPIE>/bin/kelpie --config <CONFIG_FILE> --except-process
```

## 設定ファイルフォーマット

### TOMLフォーマット
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
config_file = "<PATH_TO_SCALARDB_PROPERTIES_FILE>"
#contact_points = "localhost"
#contact_port = 9042
#username = "cassandra"
#password = "cassandra"
#storage = "cassandra"

[tpcc_config]
concurrency = 8
run_for_sec = 300
ramp_for_sec = 60
num_warehouses = 10
```

### マルチユーザーモード設定例
```toml
[modules]
[modules.preprocessor]
name = "com.scalar.db.benchmarks.ycsb.MultiUserLoader"
path = "./build/libs/scalardb-benchmarks-all.jar"
[modules.processor]
name = "com.scalar.db.benchmarks.ycsb.MultiUserWorkloadC"
path = "./build/libs/scalardb-benchmarks-all.jar"
[modules.postprocessor]
name = "com.scalar.db.benchmarks.ycsb.YcsbReporter"
path = "./build/libs/scalardb-benchmarks-all.jar"

[common]
concurrency = 4
run_for_sec = 30
ramp_for_sec = 5

[stats]
realtime_report_enabled = true

[ycsb_config]
# マルチユーザーモードの設定
user_count = 4           # 並行ユーザー数（スレッド数）
record_count = 10000     # テーブル全体のレコード数 - キー範囲の計算に使用
ops_per_tx = 1           # トランザクションあたりの操作数
load_concurrency = 4     # データロード時の並列度

[database_config]
config_file = "scalardb.properties"
```

## コード規約とパターン

### パッケージ構造
```
com.scalar.db.benchmarks
├── Common.java （共通ユーティリティ）
├── tpcc        （TPC-C関連）
│   ├── table       （テーブル定義）
│   ├── transaction （トランザクション実装）
│   ├── TpccBench.java
│   ├── TpccConfig.java
│   ├── TpccLoader.java
│   └── TpccReporter.java
└── ycsb        （YCSB関連）
    ├── Loader.java
    ├── WorkloadA.java
    ├── WorkloadC.java
    ├── WorkloadF.java
    ├── MultiStorageWorkloadC.java
    ├── MultiStorageWorkloadF.java
    ├── MultiUserLoader.java
    ├── MultiUserWorkloadC.java
    ├── YcsbCommon.java
    └── YcsbReporter.java
```

### エラーハンドリング
- トランザクション競合の適切なリトライ
- スレッドの割込みへの対応
- 適切なロギング
- ユーザー作成・権限付与の失敗に対する適切な処理
