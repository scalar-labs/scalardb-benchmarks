# ABACマルチユーザーベンチマーク実装タスクリスト

## 概要
ScalarDBのABAC（Attribute-Based Access Control）機能を活用したマルチユーザーベンチマークを実装し、属性ベースアクセス制御による性能影響を測定する。

**前提条件**: 
- ScalarDB Cluster側でABACが有効化済み
- ABAC仕様: https://scalardb.scalar-labs.com/ja-jp/docs/latest/scalardb-cluster/authorize-with-abac/
- AbacAdmin API: https://javadoc.io/static/com.scalar-labs/scalardb/3.15.3/com/scalar/db/api/AbacAdmin.html

## 主要タスク

### 1. 属性割り当て戦略の設計（実装しながら検討）
- [ ] **ユーザー属性割り当て方法の検討**
  - 固定的割り当て（再現性重視）vs ランダム割り当て
  - 負荷分散を考慮した均等分布
  - 実際のユースケースに近い分布パターン
- [ ] **データ属性割り当て方法の検討**
  - キー範囲ベースの属性割り当て
  - ランダム分布 vs 特定パターン分布
  - アクセス頻度を考慮した属性配置
- [ ] **ベンチマーク比較可能性の確保**
  - シード値による再現性の担保
  - 属性分布の設定可能性
  - 複数の戦略パターンのサポート

### 2. MultiUserAbacWorkloadCクラスの実装
- [ ] `src/main/java/com/scalar/db/benchmarks/ycsb/MultiUserAbacWorkloadC.java`の作成
- [ ] 既存のMultiUserWorkloadCをベースにしたクラス設計
- [ ] AbacAdminを使用した属性定義ロジックの実装
- [ ] 属性ベーステーブルアクセスの実装
- [ ] パフォーマンスメトリクス（ABAC処理時間）の追加測定

### 3. ABAC対応ローダーの実装
- [ ] `MultiUserAbacLoader.java`の作成
- [ ] 既存のMultiUserLoaderを拡張
- [ ] AbacAdminを使用したユーザー属性の設定機能
- [ ] AbacAdminを使用したデータ属性の設定機能
- [ ] 属性ベースポリシーの定義と適用

### 4. 設定ファイルの拡張
- [ ] ABAC用のTOML設定ファイルテンプレート作成
  - `ycsb-multi-user-abac-benchmark-config.toml`
- [ ] 属性定義パラメータの追加
- [ ] ABACポリシー設定の統合
- [ ] 設定値のバリデーション機能

### 5. テストと検証
- [ ] 単体テストの作成
- [ ] 統合テストの実装
- [ ] パフォーマンス比較テスト（通常vs ABAC）
- [ ] 負荷変動テスト（ユーザー数、属性数の変化）

### 6. ドキュメンテーション
- [ ] ABACベンチマークの使用方法ドキュメント
- [ ] 設定ファイルのサンプルと説明
- [ ] パフォーマンス分析ガイド
- [ ] トラブルシューティング情報

## 属性割り当て戦略の詳細検討

### 検討すべき戦略パターン

#### 1. 再現性重視戦略
- **目的**: 同じ条件での繰り返し測定を可能にする
- **実装**: シード値ベースの擬似ランダム割り当て
- **利点**: ベンチマーク結果の比較が容易
- **欠点**: 実際の運用パターンとの乖離の可能性

#### 2. 実ユースケース模倣戦略
- **目的**: 実際の業務パターンに近い属性分布
- **実装**: 部門別アクセス権限、データ機密度レベルなど
- **利点**: 実環境での性能予測精度が高い
- **欠点**: 汎用性が低い

#### 3. 負荷分散考慮戦略
- **目的**: ABAC処理負荷の均等分散
- **実装**: 属性値の均等分布、アクセスパターンの調整
- **利点**: システムの最大性能測定が可能
- **欠点**: 実際の不均等な負荷パターンを反映しない

### 実装時の判断ポイント

```java
// 属性割り当ての実装例
public class AttributeAssignmentStrategy {
    // 設定で切り替え可能な戦略
    public enum Strategy {
        DETERMINISTIC,    // 決定的（再現性重視）
        RANDOM,          // ランダム
        REALISTIC,       // 実ユースケース模倣
        LOAD_BALANCED    // 負荷分散考慮
    }
    
    // ユーザー属性の割り当て
    public String assignUserAttribute(int userId, Strategy strategy, long seed) {
        switch (strategy) {
            case DETERMINISTIC:
                return deterministicUserAssignment(userId, seed);
            case RANDOM:
                return randomUserAssignment(userId);
            case REALISTIC:
                return realisticUserAssignment(userId);
            case LOAD_BALANCED:
                return loadBalancedUserAssignment(userId);
            default:
                return deterministicUserAssignment(userId, seed);
        }
    }
    
    // データ属性の割り当て
    public String assignDataAttribute(int recordId, Strategy strategy, long seed) {
        // 同様の実装
    }
}
```

## 技術的詳細タスク

### コード実装
- [ ] YcsbCommonクラスにABAC設定取得メソッド追加
- [ ] ABAC関連の定数定義
- [ ] AbacAdminインスタンスの管理
- [ ] 属性ベースクエリの実装
- [ ] ABACポリシー評価のメトリクス収集
- [ ] エラーハンドリング（権限エラー、属性設定エラー）
- [ ] ThreadLocal変数でのユーザー属性管理
- [ ] **属性割り当て戦略の実装とテスト**

### AbacAdmin統合
- [ ] `AbacAdmin.createUserAttributes()` を使用したユーザー属性作成
- [ ] `AbacAdmin.createDataAttributes()` を使用したデータ属性作成
- [ ] `AbacAdmin.createPolicy()` を使用したポリシー作成
- [ ] 属性とポリシーの削除機能（テスト用）

### メトリクス収集
- [ ] ABAC処理時間の測定
- [ ] 属性評価回数のカウント
- [ ] 権限チェック失敗回数の記録
- [ ] 属性作成・削除の実行時間測定
- [ ] メモリ使用量の監視
- [ ] **属性分布の均等性メトリクス**

## 実装の詳細設計

### MultiUserAbacWorkloadC の主要機能
```java
public class MultiUserAbacWorkloadC extends TimeBasedProcessor {
    private final AbacAdmin abacAdmin;
    private final Map<String, String> userAttributes;
    private final Map<String, String> dataAttributes;
    private final String policyName;
    private final AttributeAssignmentStrategy assignmentStrategy;
    
    // 属性とポリシーの初期化
    private void initializeAbacSettings() { ... }
    
    // 属性ベースでのクエリ実行
    private void executeWithAbac() { ... }
    
    // ABAC固有のメトリクス収集
    private void collectAbacMetrics() { ... }
}
```

### MultiUserAbacLoader の主要機能
```java
public class MultiUserAbacLoader extends MultiUserLoader {
    private final AbacAdmin abacAdmin;
    private final AttributeAssignmentStrategy assignmentStrategy;
    
    // ユーザー属性の設定
    private void setupUserAttributes() { ... }
    
    // データ属性の設定  
    private void setupDataAttributes() { ... }
    
    // ABACポリシーの作成
    private void createAbacPolicies() { ... }
}
```

### 設定例（属性戦略を含む）
```toml
[modules]
[modules.preprocessor]
name = "com.scalar.db.benchmarks.ycsb.MultiUserAbacLoader"
path = "./build/libs/scalardb-benchmarks-all.jar"
[modules.processor]
name = "com.scalar.db.benchmarks.ycsb.MultiUserAbacWorkloadC"
path = "./build/libs/scalardb-benchmarks-all.jar"
[modules.postprocessor]
name = "com.scalar.db.benchmarks.ycsb.YcsbReporter"
path = "./build/libs/scalardb-benchmarks-all.jar"

[common]
concurrency = 4
run_for_sec = 300
ramp_for_sec = 60

[ycsb_config]
user_count = 4
record_count = 10000
ops_per_tx = 1
load_concurrency = 4

# ABAC固有の設定
abac_enabled = true
user_attribute_name = "department"
user_attribute_values = ["sales", "engineering", "marketing", "support"]
data_attribute_name = "sensitivity"
data_attribute_values = ["public", "internal", "confidential"]
policy_name = "department_access_policy"

# 属性割り当て戦略の設定
attribute_assignment_strategy = "DETERMINISTIC"  # DETERMINISTIC, RANDOM, REALISTIC, LOAD_BALANCED
assignment_seed = 12345  # 再現性のためのシード値
realistic_distribution = { "sales" = 0.3, "engineering" = 0.4, "marketing" = 0.2, "support" = 0.1 }

[database_config]
config_file = "scalardb.properties"
```

## 期待される成果物

1. **MultiUserAbacWorkloadC.java**
   - ABACを有効にしたWorkload Cの実装
   - 複数の属性割り当て戦略をサポート
   - 既存のMultiUserWorkloadCとの性能比較が可能

2. **MultiUserAbacLoader.java**
   - ABAC環境用のデータローダー
   - AbacAdminを使用した属性・ポリシー設定機能
   - 柔軟な属性割り当て戦略

3. **設定ファイル**
   - `ycsb-multi-user-abac-benchmark-config.toml`
   - 属性戦略を含むABAC設定のサンプル

4. **テストケース**
   - 各属性戦略の機能テスト
   - パフォーマンステスト

5. **ドキュメント**
   - 属性戦略の選択ガイド
   - パフォーマンス分析結果

## 成功基準

- [ ] ABAC有効時と無効時の性能比較が可能
- [ ] 複数の属性割り当て戦略をサポート
- [ ] 属性数・ユーザー数の変化による性能影響が測定可能
- [ ] AbacAdminを使用した属性・ポリシー管理が正常動作
- [ ] ベンチマーク結果の再現性を担保
- [ ] 既存のベンチマークフレームワークとの一貫性を保持
- [ ] 設定とセットアップが簡単
- [ ] 詳細な性能メトリクスが取得可能

## リスク要因と対策

### 技術的リスク
- **属性・ポリシー設定の複雑さ**: AbacAdmin APIの活用と段階的実装
- **性能オーバーヘッド**: ベースライン測定とボトルネック分析
- **設定エラー**: バリデーション機能と適切なエラーメッセージ
- **属性戦略の妥当性**: 複数パターンの実装と比較測定

### 互換性リスク
- **ScalarDBバージョン依存**: AbacAdmin APIの対応バージョン確認
- **既存コードとの統合**: 既存パターンの踏襲と段階的統合

## 実装スケジュール（Vibe Coding見積もり版）

**総トークン数見積もり: 約 120K-180K トークン**

### Phase 1: AbacAdmin統合と属性戦略の基本設計
**見積もり**: 15K-25K トークン
- AbacAdmin APIの統合ロジック: 8K-12K トークン
- AttributeAssignmentStrategy enum と基本実装: 5K-8K トークン
- 設定読み込みロジックの拡張: 2K-5K トークン

### Phase 2: MultiUserAbacWorkloadCの実装
**見積もり**: 40K-60K トークン
- 既存MultiUserWorkloadCのコピー・ベース: 15K トークン
- ABAC固有のロジック追加: 15K-25K トークン
- 属性戦略の統合: 8K-12K トークン
- メトリクス収集機能: 2K-8K トークン

### Phase 3: MultiUserAbacLoaderの実装とテスト
**見積もり**: 35K-50K トークン
- 既存MultiUserLoaderの拡張: 10K トークン
- AbacAdminを使用した属性・ポリシー設定: 15K-25K トークン
- データ属性の割り当てロジック: 8K-12K トークン
- テストケースの作成: 2K-3K トークン

### Phase 4: 設定ファイルとドキュメント作成
**見積もり**: 30K-45K トークン
- TOML設定ファイルのテンプレート: 5K-8K トークン
- 使用方法ドキュメント: 15K-25K トークン
- パフォーマンス分析ガイド: 8K-10K トークン
- トラブルシューティング情報: 2K-2K トークン

### トークン数の内訳
- **コア実装**: 80K-120K トークン (約67%)
- **設定・ドキュメント**: 40K-60K トークン (約33%)

### 効率化要因
- 既存コードのコピー・ベースによる開始: -20K トークン
- 段階的実装によるデバッグ効率化: -10K トークン
- Vibe coding による直感的実装: +/-10K トークン（状況による）

**実際の消費トークン数は、実装中の試行錯誤、デバッグ、リファクタリングの度合いにより変動します**

## 実装中の判断事項メモ

### Phase 1で決定すべき事項
- [ ] 初期実装では何種類の属性戦略をサポートするか
- [ ] デフォルトの属性戦略はどれにするか
- [ ] 属性値の種類数の最適値は何個か

### Phase 2で決定すべき事項
- [ ] 属性評価のパフォーマンス測定精度をどこまで細かくするか
- [ ] メモリ使用量の監視方法
- [ ] エラー発生時の戦略切り替え機能の必要性

### Phase 3で決定すべき事項
- [ ] 大量データロード時の属性設定の効率化方法
- [ ] テスト用の属性削除のタイミング
- [ ] 複数戦略での結果比較の自動化レベル

## Vibe Coding メモ

- **トークン効率**: 既存コードベースを参考にした実装で効率アップ
- **反復改善**: 各Phase で動作確認しながらの段階的改善
- **直感的実装**: 最初は単純な実装で動作させ、後で最適化
- **ドキュメント**: 実装しながらリアルタイムで更新
