package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.CONFIG_NAME;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.OPS_PER_TX;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPassword;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getRecordCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getUserCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getUserName;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.prepareGet;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

import javax.json.Json;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.benchmarks.Common;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.TransactionFactory;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;

/**
 * マルチユーザーモードのWorkload C: 複数ユーザー（スレッド）による並列読み取り。
 * 各スレッドに固有のキー範囲が割り当てられ、その範囲内からランダムに読み取りを行います。
 */
public class MultiUserWorkloadC extends TimeBasedProcessor {
    private static final long DEFAULT_OPS_PER_TX = 2; // two read operations
    private final DistributedTransactionManager manager;
    private final int recordCount;
    private final int opsPerTx;
    private final int userCount;
    private final ThreadLocal<KeyRange> threadLocalKeyRange;

    private final LongAdder transactionRetryCount = new LongAdder();

    // PASSWORD_BASE定数はYcsbCommonに移動しました
    // 作成したユーザーごとにトランザクションマネージャーを保持
    private final List<DistributedTransactionManager> userManagers = new ArrayList<>();
    private final ThreadLocal<Integer> threadLocalUserId;

    public MultiUserWorkloadC(Config config) {
        super(config);
        this.recordCount = getRecordCount(config);
        this.opsPerTx = (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
        this.userCount = getUserCount(config);

        // 管理者トランザクションマネージャー（バックアップとして）
        this.manager = Common.getTransactionManager(config);

        // ScalarDBプロパティをコピーして各ユーザー用のトランザクションマネージャーを作成
        createUserManagers(config);

        // スレッドごとにキー範囲とユーザーIDを割り当てるためのThreadLocal変数
        this.threadLocalUserId = new ThreadLocal<>();
        this.threadLocalKeyRange = ThreadLocal.withInitial(() -> {
            // スレッドIDを取得（ハッシュコードを利用）
            int threadId = Math.abs(Thread.currentThread().getName().hashCode() % userCount);
            // このスレッドのユーザーIDを保存
            threadLocalUserId.set(threadId);
            // このスレッドのキー範囲を計算して返す
            return calculateKeyRange(threadId, userCount, recordCount);
        });
    }

    /**
     * 各ユーザー用のトランザクションマネージャーを作成します。
     * ScalarDB設定プロパティを用いて認証情報を渡します。
     */
    private void createUserManagers(Config config) {
        // ScalarDBの設定を取得
        DatabaseConfig dbConfig = Common.getDatabaseConfig(config);
        Properties baseProps = dbConfig.getProperties();

        // 接続情報をログ出力
        String contactPoints = baseProps.getProperty("scalar.db.contact_points", "");
        logInfo("Creating user managers for endpoint: " + contactPoints);

        // 各ユーザー用のトランザクションマネージャーを作成
        for (int i = 0; i < userCount; i++) {
            String username = getUserName(i);
            String password = getPassword(i);

            try {
                // ユーザー固有の認証情報でプロパティを作成
                Properties userProps = new Properties();
                userProps.putAll(baseProps);
                userProps.setProperty("scalar.db.username", username);
                userProps.setProperty("scalar.db.password", password);

                // トランザクションマネージャーの作成（標準的なTransactionFactory経由）
                TransactionFactory factory = TransactionFactory.create(userProps);
                DistributedTransactionManager userManager = factory.getTransactionManager();
                userManagers.add(userManager);

                logInfo("Created transaction manager for user: " + username);
            } catch (Exception e) {
                logWarn("Failed to create transaction manager for user " + username + ": " + e.getMessage());
            }
        }

        if (userManagers.isEmpty()) {
            logWarn("No user transaction managers created, will use admin manager");
        } else {
            logInfo("Created " + userManagers.size() + " user transaction managers");
        }
    }

    @Override
    public void executeEach() throws TransactionException {
        // このスレッドに割り当てられたキー範囲を取得
        KeyRange range = threadLocalKeyRange.get();
        Random random = ThreadLocalRandom.current();

        // トランザクション内で実行する操作（GET）の対象キーをランダムに選択
        List<Integer> userIds = new ArrayList<>(opsPerTx);
        for (int i = 0; i < opsPerTx; ++i) {
            // 割り当てられた範囲内からランダムにキーを選択
            int key = range.startKey + random.nextInt(range.endKey - range.startKey + 1);
            userIds.add(key);
        }

        // このスレッドのユーザーIDに対応するトランザクションマネージャーを取得
        Integer userIndex = threadLocalUserId.get();
        DistributedTransactionManager txManager;
        if (userIndex != null && userIndex < userManagers.size() && userManagers.get(userIndex) != null) {
            txManager = userManagers.get(userIndex);
        } else {
            // ユーザーのトランザクションマネージャーが利用できない場合は管理者のを使用
            txManager = manager;
            logWarn("Using admin transaction manager for thread " + Thread.currentThread().getName());
        }

        // トランザクションの実行
        while (true) {
            DistributedTransaction transaction = txManager.start();
            try {
                for (Integer userId : userIds) {
                    transaction.get(prepareGet(userId));
                }
                transaction.commit();
                break;
            } catch (CrudConflictException | CommitConflictException e) {
                transaction.abort();
                transactionRetryCount.increment();
            } catch (Exception e) {
                transaction.abort();
                throw e;
            }
        }
    }

    @Override
    public void close() {
        try {
            // 作成したすべてのユーザートランザクションマネージャーを閉じる
            for (DistributedTransactionManager userManager : userManagers) {
                try {
                    if (userManager != null) {
                        userManager.close();
                    }
                } catch (Exception e) {
                    logWarn("Failed to close user transaction manager", e);
                }
            }

            // 管理者トランザクションマネージャーを閉じる
            manager.close();
        } catch (Exception e) {
            logWarn("Failed to close the transaction manager", e);
        }

        setState(
                Json.createObjectBuilder()
                        .add("transaction-retry-count", transactionRetryCount.toString())
                        .add("user-count", String.valueOf(userCount))
                        .build());
    }

    /**
     * 各スレッドに割り当てるキー範囲を計算します。
     * 
     * @param threadId    スレッドID
     * @param userCount   合計ユーザー数（スレッド数）
     * @param recordCount 総レコード数
     * @return キー範囲
     */
    private KeyRange calculateKeyRange(int threadId, int userCount, int recordCount) {
        if (threadId >= userCount) {
            throw new IllegalArgumentException("Thread ID must be less than user count");
        }

        int rangeSize = recordCount / userCount;
        // 最後のスレッドには端数も含める
        if (threadId == userCount - 1) {
            return new KeyRange(
                    threadId * rangeSize,
                    recordCount - 1);
        } else {
            return new KeyRange(
                    threadId * rangeSize,
                    (threadId + 1) * rangeSize - 1);
        }
    }

    /**
     * スレッドに割り当てられたキー範囲を表すクラス
     */
    private static class KeyRange {
        final int startKey; // 範囲の開始キー（含む）
        final int endKey; // 範囲の終了キー（含む）

        KeyRange(int startKey, int endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
        }
    }
}
