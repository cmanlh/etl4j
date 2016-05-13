package com.lifeonwalden.etl4j.exe;

import java.sql.SQLException;
import java.util.List;

import com.lifeonwalden.etl4j.workflow.Load;

public class LoadExecutor implements Runnable {
  private Buffer buffer;

  private Load load;

  public LoadExecutor(Buffer buffer, Load load) {
    this.buffer = buffer;
    this.load = load;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    try {
      Object item = buffer.take();
      if (item instanceof EndPoint) {
        load.close();
      } else {
        load.writeAtIndex((List<Object>) item);
      }
    } catch (SQLException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
