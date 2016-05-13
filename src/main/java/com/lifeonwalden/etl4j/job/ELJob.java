package com.lifeonwalden.etl4j.job;

import com.lifeonwalden.etl4j.workflow.Extract;
import com.lifeonwalden.etl4j.workflow.Load;

public class ELJob implements Runnable {
  private Load load;

  private Extract extract;

  public ELJob(Load load, Extract extract) {
    this.load = load;
    this.extract = extract;
  }

  @Override
  public void run() {

  }
}
