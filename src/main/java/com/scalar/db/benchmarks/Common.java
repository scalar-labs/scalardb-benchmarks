package com.scalar.db.benchmarks;

import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.TransactionFactory;
import com.scalar.kelpie.config.Config;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class Common {
  private static final String CONFIG_NAME = "database_config";
  private static final int WAIT_MILLS = 1000;
  private static final int MAX_RETRIES = 10;

  public static DatabaseConfig getDatabaseConfig(Config config) {
    String configFile;
    if (config.hasUserValue(CONFIG_NAME, "config_file")) {
      configFile = config.getUserString(CONFIG_NAME, "config_file");
    } else {
      configFile = null;
    }
    if (configFile != null) {
      try {
        return new DatabaseConfig(new File(configFile));
      } catch (IOException e) {
        throw new RuntimeException("failed to load the specified config file: " + configFile, e);
      }
    }

    String contactPoints = config.getUserString(CONFIG_NAME, "contact_points", "localhost");
    long contactPort = config.getUserLong(CONFIG_NAME, "contact_port", 0L);
    String username = config.getUserString(CONFIG_NAME, "username", "cassandra");
    String password = config.getUserString(CONFIG_NAME, "password", "cassandra");
    String storage = config.getUserString(CONFIG_NAME, "storage", "cassandra");
    String isolationLevel = config.getUserString(CONFIG_NAME, "isolation_level", "SNAPSHOT");
    String transactionManager =
        config.getUserString(CONFIG_NAME, "transaction_manager", "consensus-commit");
    String serializableStrategy =
        config.getUserString(CONFIG_NAME, "serializable_strategy", "EXTRA_READ");

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", contactPoints);
    if (contactPort > 0) {
      props.setProperty("scalar.db.contact_port", Long.toString(contactPort));
    }
    props.setProperty("scalar.db.username", username);
    props.setProperty("scalar.db.password", password);
    props.setProperty("scalar.db.storage", storage);
    props.setProperty("scalar.db.transaction_manager", transactionManager);
    props.setProperty("scalar.db.consensus_commit.isolation_level", isolationLevel);
    props.setProperty("scalar.db.consensus_commit.serializable_strategy", serializableStrategy);
    return new DatabaseConfig(props);
  }

  public static DistributedTransactionManager getTransactionManager(Config config) {
    DatabaseConfig dbConfig = getDatabaseConfig(config);
    TransactionFactory factory = TransactionFactory.create(dbConfig.getProperties());
    return factory.getTransactionManager();
  }

  public static Retry getRetryWithFixedWaitDuration(String name) {
    return getRetryWithFixedWaitDuration(name, MAX_RETRIES, WAIT_MILLS);
  }

  public static Retry getRetryWithFixedWaitDuration(String name, int maxRetries, int waitMillis) {
    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(maxRetries)
            .waitDuration(Duration.ofMillis(waitMillis))
            .build();

    return Retry.of(name, retryConfig);
  }
}
