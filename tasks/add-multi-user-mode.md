
## 🎯 目的
同一テーブルに対して、**複数ユーザー（スレッド）が同時に異なるレコードをREAD**するベンチマークを行い、**ユーザー数増加時のスループットとレイテンシの変化**を評価する。

## 📌 要求事項
- 設定ファイルからユーザー数（スレッド数）を指定できること
- 各ユーザーは異なるレコードを同時にREADすること
- ScalarDB Cluster / Core どちらにも対応できるが、Cluster切替は既に `.properties` によって可能であるため、本設計では対象外とする

## 🧩 設計方針

### 1. 設定ファイルの拡張（例: `benchmark-config.yaml`）

#### 🔒 制約と仕様
- `user_count` は `record_count` 以下である必要があります。
- `record_count` が `user_count` より多い場合、**各スレッドには `record_count` を `user_count` で等分した範囲**が割り当てられ、その範囲内からランダムにキーを選んでREADを行います。
  - 例: `record_count: 1000`, `user_count: 10` の場合、スレッド0はキー1～100、スレッド1は101～200…といった割当になり、それぞれの範囲から毎回ランダムに1キーを選びます。
  - この方式により、全体のレコードを偏りなく活用でき、ホットスポットを避けた読み取り分散が可能です。
```yaml
user_count: 100            # 並行ユーザー数（スレッド数）
operation: "read"          # 実行操作
table: "usertable"         # 対象テーブル
record_count: 10000        # テーブル全体のレコード数（キーの割当用）
```
- 各スレッドに異なるキーを割り当てるため、`record_count` をキー範囲として使用。

### 2. ベンチマーク実行ロジック
- ベンチマーク起動時に `user_count` 分のスレッドを起動。
- 各スレッドに割り当てられた範囲内でランダムなキーを選択して `Get` を実行。
- スレッドはループで読み取りリクエストを継続送信し、測定時間が終了したら停止。

```java
for (int i = 0; i < userCount; i++) {
  final int threadId = i;
  new Thread(() -> {
    int rangeSize = recordCount / userCount;
    int startKey = threadId * rangeSize + 1;
    int endKey = (threadId + 1) * rangeSize;
    Random rand = new Random();

    while (!isBenchmarkDone()) {
      int key = startKey + rand.nextInt(endKey - startKey + 1);
      scalardb.get(key);  // 各スレッドは割り当てられた範囲からランダムにキーを選ぶ
    }
  }).start();
}
```

- **レイテンシやスループットの集計**は既存のKelpie統計機能を活用。

### 3. 出力と評価指標
- スループット（トランザクション/秒）
- 各種レイテンシ（平均、p95、p99）
- スレッド数ごとの結果を比較し、スケーラビリティを分析

---

## 🔍 設計整合性の調査結果

### ✅ 設定ファイル駆動の設計
- TOMLベースの構成ファイルでベンチマークモジュールやパラメータ（concurrency、record_countなど）を完全に制御
- 変更は設定ファイル中心。コード修正なしに柔軟に構成変更可能

### ✅ 並列実行の制御
- `concurrency` パラメータによりスレッド数を制御（デフォルト1）
- Kelpieフレームワークがスレッドプールを生成し、`Processor#execute` を並列実行
- スレッドセーフな設計が求められる（Atomicなど）

### ✅ Kelpieとの親和性
- KelpieはPreProcessor → Processor → PostProcessor という段階構造
- Processorモジュール内で `Config` 経由で `user_count`, `record_count` など任意の値を取得可
- YCSB / TPC-C など既存モジュールもこの構成に従っており、カスタムワークロードの追加は容易

### ✅ 整合性の結論
- `user_count` を設定ファイルから読み取り、Kelpieの並列実行モデルに組み込む設計は **設計思想と完全に整合**
- スレッドごとに異なるキー範囲を処理させるワークロードは、既存のWorkload C（読み取り専用）や Workload F（分散アクセス）と同様の思想で実現可能
- モジュールとして `ReadByUserCountProcessor` のような形で追加し、設定ファイルで切り替え可能にすればOK

---

## ✅ 備考
- ScalarDB Cluster への接続切替は `.properties` で既に対応済みのため、本設計では対象外
- モジュール設計方針に従って、Processor クラスを作成すれば、Kelpieが並列実行・統計集計・ベンチマーク制御を行ってくれる

この設計は、ScalarDB の拡張性・スケーラビリティ評価に有効であり、既存ベンチマーク構造にも完全に適合します。
