package com.scalar.db.benchmarks.ycsb;

import static com.scalar.db.benchmarks.ycsb.YcsbCommon.NAMESPACE;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.TABLE;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadConcurrency;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getLoadOverwrite;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPassword;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getPayloadSize;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getUserCount;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.getUserName;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.preparePut;
import static com.scalar.db.benchmarks.ycsb.YcsbCommon.randomFastChars;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.json.Json;

import com.scalar.db.api.AuthAdmin.Privilege;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.benchmarks.Common;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.service.TransactionFactory;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;

/**
 * プリプロセッサ：レコードをロードし、ScalarDBユーザーを作成します
 */
public class MultiUserLoader extends PreProcessor {
    private static final int REPORTING_INTERVAL = 10000;
    // PASSWORD_BASE定数はYcsbCommonに移動しました
    private final DatabaseConfig dbConfig;
    private final int recordCount;
    private final int loadConcurrency;
    private final boolean overwrite;
    private final int payloadSize;
    private final int userCount;
    private final AtomicBoolean canceled = new AtomicBoolean(false);
    private final AtomicInteger numFinished = new AtomicInteger(0);

    public MultiUserLoader(Config config) {
        super(config);
        dbConfig = Common.getDatabaseConfig(config);
        loadConcurrency = getLoadConcurrency(config);
        recordCount = YcsbCommon.getRecordCount(config);
        overwrite = getLoadOverwrite(config);
        payloadSize = getPayloadSize(config);
        userCount = getUserCount(config);
    }

    private Exception error;

    @Override
    public void execute() {
        try {
            logInfo("Starting MultiUserLoader");
            ExecutorService es = Executors.newFixedThreadPool(loadConcurrency);

            // ScalarDBユーザーの作成
            createScalarDbUsers();

            // レコードのロード
            loadRecords(es);

            logInfo("Finished loading");
        } catch (Exception e) {
            error = e;
            logError("loading error", e);
        }

        setState(Json.createObjectBuilder().build());
    }

    @Override
    public void close() {
        // このクラスでは特に閉じるリソースがないため何もしない
    }

    private void createScalarDbUsers() throws Exception {
        logInfo("Creating ScalarDB users: " + userCount);

        // AdminインターフェースからScalarDBユーザーを作成
        TransactionFactory factory = TransactionFactory.create(dbConfig.getProperties());
        DistributedTransactionAdmin admin = factory.getTransactionAdmin();

        try {
            // 各ユーザーを作成
            for (int i = 0; i < userCount; i++) {
                String username = getUserName(i);
                String password = getPassword(i);

                try {
                    // 既存ユーザーを削除（存在しない場合のエラーは無視）
                    logInfo("Dropping existing user: " + username);
                    try {
                        admin.dropUser(username);
                    } catch (Exception e) {
                        // ユーザーが存在しない場合は無視
                        logInfo("User " + username + " might not exist, proceeding to create it");
                    }

                    // ユーザー作成 - createUserメソッドを直接使用
                    logInfo("Creating user: " + username);
                    admin.createUser(username, password);

                    // テーブルへの権限付与
                    logInfo("Granting table privileges to " + username + " on " + NAMESPACE + "." + TABLE);
                    admin.grant(username, NAMESPACE, TABLE, Privilege.READ, Privilege.WRITE);

                    logInfo("Successfully created user: " + username + " with all required permissions");
                } catch (Exception e) {
                    logWarn("Failed to create user " + username + ": " + e.getMessage());
                    throw new RuntimeException("Failed to create user " + username, e);
                }
            }
        } finally {
            // 管理者リソースを閉じる
            try {
                admin.close();
            } catch (Exception e) {
                logWarn("Error closing admin manager: " + e.getMessage());
            }
        }

        logInfo("Created " + userCount + " ScalarDB users");
    }

    // モックメソッドを削除（実際のAPIを直接使用するようになったため）

    private void loadRecords(ExecutorService es) {
        logInfo("Loading " + recordCount + " records with concurrency " + loadConcurrency);
        int numThreads = loadConcurrency;
        int recordsPerThread = recordCount / numThreads;

        List<CompletableFuture> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            final int start = i * recordsPerThread;
            final int end = (i == numThreads - 1) ? recordCount : (i + 1) * recordsPerThread;

            futures.add(
                    CompletableFuture.runAsync(
                            () -> loadRange(threadId, start, end), es));
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0]));

        try {
            allFutures.join();
        } catch (Exception e) {
            canceled.set(true);
            error = e;
            throw e;
        } finally {
            es.shutdown();
        }
    }

    private void loadRange(int threadId, int startInclusive, int endExclusive) {
        long startTime = System.currentTimeMillis();
        logInfo(
                "Thread "
                        + threadId
                        + " loading records from "
                        + startInclusive
                        + " to "
                        + (endExclusive - 1));

        Random random = new Random();
        int remaining = endExclusive - startInclusive;
        int loaded = 0;

        // 管理者として接続してデータロード
        TransactionFactory factory = TransactionFactory.create(dbConfig.getProperties());
        DistributedTransactionManager manager = factory.getTransactionManager();
        try {
            // テーブルが存在するか確認
            try {
                DistributedTransaction tx = manager.start();
                try {
                    // テーブル存在確認時は適当なキーでGetを試みる
                    tx.get(Get.newBuilder()
                            .namespace(NAMESPACE)
                            .table(TABLE)
                            .partitionKey(com.scalar.db.io.Key.ofInt(YcsbCommon.YCSB_KEY, 0))
                            .build());
                    tx.commit();
                    logInfo("Table " + NAMESPACE + "." + TABLE + " already exists");
                } catch (Exception e) {
                    tx.abort();
                    logInfo("Table doesn't exist, will be created by schema loader");
                }
            } catch (TransactionException e) {
                logInfo("Table doesn't exist, will be created by schema loader: " + e.getMessage());
            }

            // データをロード
            for (int i = startInclusive; i < endExclusive; i++) {
                char[] payload = new char[payloadSize];
                randomFastChars(random, payload);

                try {
                    DistributedTransaction tx = manager.start();
                    try {
                        // データの存在チェック（オーバーライド設定）
                        if (overwrite) {
                            tx.put(preparePut(i, String.valueOf(payload)));
                        } else {
                            try {
                                tx.get(Get.newBuilder()
                                        .namespace(NAMESPACE)
                                        .table(TABLE)
                                        .partitionKey(com.scalar.db.io.Key.ofInt(YcsbCommon.YCSB_KEY, i))
                                        .build());
                                // 既に存在する場合は何もしない
                            } catch (CrudException e) {
                                tx.put(preparePut(i, String.valueOf(payload)));
                            }
                        }
                        tx.commit();

                        // 進捗報告
                        loaded++;
                        remaining--;
                        if (loaded % REPORTING_INTERVAL == 0) {
                            long elapsed = System.currentTimeMillis() - startTime;
                            double rate = loaded * 1000.0 / elapsed;
                            logInfo(
                                    "Thread "
                                            + threadId
                                            + " loaded "
                                            + loaded
                                            + " records, "
                                            + remaining
                                            + " remaining"
                                            + String.format(" (%.2f records/second)", rate));
                        }
                    } catch (Exception e) {
                        tx.abort();
                        throw e;
                    }
                } catch (Exception e) {
                    if (canceled.get()) {
                        logInfo("Thread " + threadId + " cancelled");
                        return;
                    }
                    throw e;
                }
            }

            // 完了報告
            int finished = numFinished.incrementAndGet();
            logInfo("Thread " + threadId + " finished loading.");
            if (finished == loadConcurrency) {
                long elapsed = System.currentTimeMillis() - startTime;
                double rate = (endExclusive - startInclusive) * 1000.0 / elapsed;
                logInfo(
                        "Loading complete: " + recordCount + " records loaded in " + elapsed / 1000.0 + " seconds"
                                + String.format(" (%.2f records/second)", rate));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error loading data for thread " + threadId, e);
        } finally {
            try {
                manager.close();
            } catch (Exception e) {
                logWarn("Failed to close transaction manager for thread " + threadId);
            }
        }
    }
}
