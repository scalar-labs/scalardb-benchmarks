package com.scalar.db.benchmarks;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.IllegalConfigException;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class Common {
  private static final String CONFIG_NAME = "database_config";

  public static DatabaseConfig getDatabaseConfig(Config config) {
    String configFile;
    try {
      configFile = config.getUserString(CONFIG_NAME, "config_file");
    } catch (IllegalConfigException e) {
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
}
