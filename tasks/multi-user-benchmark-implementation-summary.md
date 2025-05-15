# マルチユーザーモード実装サマリー

## 実装したもの

1. **新規ファイル**
   - `src/main/java/com/scalar/db/benchmarks/ycsb/MultiUserLoader.java`
     - マルチユーザー環境用のデータロードとユーザー作成を行う
   - `src/main/java/com/scalar/db/benchmarks/ycsb/MultiUserWorkloadC.java`
     - 複数ユーザーによる並列読み取りベンチマークを実行する
   - `ycsb-multi-user-benchmark-config.toml`
     - マルチユーザーモード用の設定ファイル

2. **既存ファイル拡張**
   - `src/main/java/com/scalar/db/benchmarks/ycsb/YcsbCommon.java`
     - `USER_COUNT` パラメータの追加
     - `getUserCount()` メソッドの追加

## 実装内容

1. **ユーザー管理機能**
   - `DistributedTransactionAdmin`を使用したScalarDBユーザーの動的作成
   - 既存ユーザーの削除と再作成
   - 名前空間とテーブルへの権限付与

2. **キー範囲分割機能**
   - レコード総数をユーザー数で等分割する計算ロジック
   - スレッド固有のキー範囲を管理するThreadLocalの実装
   - ユーザーごとに固有のキー範囲内でのランダムアクセス

3. **設定パラメータ**
   - `user_count`: 並行ユーザー数設定（デフォルトはconcurrencyと同値）
   - `record_count`: レコード総数（キー範囲計算に使用）
   - `ops_per_tx`: トランザクションあたりの操作数

## 動作確認

- 各ユーザー用のトランザクションマネージャーを作成し、並行アクセスを実行
- 負荷テストの実行に成功
- 異なるユーザー数での比較テストが可能なことを確認

## ビルドと実行方法

```bash
# ビルド
./gradlew shadowJar

# 実行
./kelpie/bin/kelpie --config ycsb-multi-user-benchmark-config.toml
```
