package com.lifeonwalden.etl4j.exe;

import java.sql.SQLException;
import java.util.List;

import com.lifeonwalden.etl4j.workflow.Extract;

public class ExtractExecutor implements Runnable {
  private Buffer buffer;

  private Extract extract;

  private List<Object> paramList;

  public ExtractExecutor(Buffer buffer, Extract extract, List<Object> paramList) {
    this.buffer = buffer;
    this.extract = extract;
    this.paramList = paramList;
  }

  @Override
  public void run() {
    try {
      extract.query(paramList);
      while (extract.next()) {
        buffer.put(extract.fetchNextWithIndex());
      }
      buffer.put(new EndPoint());
      extract.close();
    } catch (SQLException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
