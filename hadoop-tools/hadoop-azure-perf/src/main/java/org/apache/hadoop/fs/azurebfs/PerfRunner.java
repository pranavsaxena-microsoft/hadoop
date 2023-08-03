package org.apache.hadoop.fs.azurebfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfRunner {
  PerfTestAzureSetup setup = new PerfTestAzureSetup();

  private static String TEST_PATH = "/testfile";

  static Logger LOG =
      LoggerFactory.getLogger(PerfRunner.class);

  public PerfRunner() throws Exception {
    setup = new PerfTestAzureSetup();
    setup.setup();
  }

  public static void main(String[] args) throws Exception {
    PerfRunner perfRunner = new PerfRunner();

  }

}
