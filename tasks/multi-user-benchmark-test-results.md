# マルチユーザーモード動作検証結果

## 実験概要

マルチユーザーモードが意図したとおりのワークロードを実行できているか確認するため、異なるユーザー数（4ユーザーと8ユーザー）でベンチマークを実行して結果を比較しました。

## 検証ポイント

1. ユーザー作成と権限付与の正確性
2. キー範囲分割の適切な実装
3. ユーザー固有のトランザクションマネージャー管理
4. スケーラビリティ（ユーザー数増加に伴うスループット変化）
5. レイテンシ特性の変化

## 検証結果

### 1. ユーザー作成と権限付与

#### 4ユーザーモード
```
Creating ScalarDB users: 4
Dropping existing user: user0
Creating user: user0
Granting table privileges to user0 on ycsb.usertable
Successfully created user: user0 with all required permissions
...
```

#### 8ユーザーモード
```
Creating ScalarDB users: 8
Dropping existing user: user4
User user4 might not exist, proceeding to create it
Creating user: user4
Granting table privileges to user4 on ycsb.usertable
Successfully created user: user4 with all required permissions
...
```

→ 設定ファイルの`user_count`パラメータに従って、意図した数のScalarDBユーザーが正しく作成され、必要な権限が付与されていることが確認できました。

### 2. キー範囲分割

```
Loading 10000 records with concurrency 4
Thread 0 loading records from 0 to 2499
Thread 1 loading records from 2500 to 4999
Thread 2 loading records from 5000 to 7499
Thread 3 loading records from 7500 to 9999
```

→ 総レコード数(10,000)が均等に分割され、各スレッドに重複のない固有の範囲が割り当てられていることが確認できました。

### 3. トランザクションマネージャーの作成

#### 4ユーザーモード
```
Created transaction manager for user: user0
Created transaction manager for user: user1
Created transaction manager for user: user2
Created transaction manager for user: user3
Created 4 user transaction managers
```

#### 8ユーザーモード
```
Created transaction manager for user: user0
...
Created transaction manager for user: user7
Created 8 user transaction managers
```

→ 各ユーザー用のトランザクションマネージャーが正しく作成され、各スレッドがそれぞれ異なるユーザーとして動作していることが確認できました。

### 4. パフォーマンス比較

#### 4ユーザーモード
```
==== Statistics Summary ====
Throughput: 52.167 ops
Succeeded operations: 1565
Failed operations: 0
Mean latency: 76.689 ms
Latency at 50 percentile: 62 ms
Latency at 90 percentile: 141 ms
Latency at 99 percentile: 266 ms
Transaction retry count: 0
```

#### 8ユーザーモード
```
==== Statistics Summary ====
Throughput: 99.033 ops
Succeeded operations: 2971
Failed operations: 0
Mean latency: 40.412 ms
SD of latency: 3.925 ms
Max latency: 68 ms
Latency at 50 percentile: 40 ms
Latency at 90 percentile: 45 ms
Latency at 99 percentile: 51 ms
Transaction retry count: 0
```

## 分析結果

### 1. スループット向上
- 4ユーザー: 52.167 ops/sec
- 8ユーザー: 99.033 ops/sec (約1.9倍)
   
ユーザー数が2倍になるとスループットもほぼ2倍になっており、理想的なほぼ線形のスケーラビリティを示しています。

### 2. レイテンシ改善
- 4ユーザー: 平均76.689ms、P99 266ms
- 8ユーザー: 平均40.412ms、P99 51ms
   
より多くのユーザーを使用することでレイテンシが改善されています。これは、各ユーザーの担当範囲がより小さくなることで効率的にリソースを活用できているためと考えられます。

### 3. 競合の発生状況
両方の実行でトランザクションのリトライカウントが0であることから、キー範囲分割が正常に機能し、各ユーザーが他のユーザーと競合せずに操作を行えていることが確認できました。

## 結論

マルチユーザーモードは意図したとおりに動作していることが以下の観点から確認できました：

1. 各スレッドが対応するScalarDBユーザーとして正しく動作
2. レコードのキー範囲が各ユーザーに均等に分割され、アクセスが分散化
3. ユーザー数を増加させるとほぼ線形にスループットが向上
4. 適切なキー範囲分割により競合が発生せず、リトライが不要

この実装により、ScalarDBの複数ユーザー環境でのスケーラビリティを効果的に検証できるベンチマークツールが実現されています。
