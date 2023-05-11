package com.scalar.db.benchmarks.tpcc;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PostProcessor;
import com.scalar.kelpie.stats.Stats;

public class TpccReporter extends PostProcessor {

  public TpccReporter(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    Stats stats = getStats();
    if (stats == null) {
      return;
    }
    getSummary();
    logInfo(
        "==== Statistics Details ====\n"
            + "Transaction abort count: "
            + getPreviousState().getString("abort_count")
            + "\n");
  }

  @Override
  public void close() {}
}
