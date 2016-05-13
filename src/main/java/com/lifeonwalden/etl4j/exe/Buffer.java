package com.lifeonwalden.etl4j.exe;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Buffer {
  public BlockingQueue<Object> buffer;

  public Buffer(int bufferSize) {
    this.buffer = new ArrayBlockingQueue<>(bufferSize);
  }

  public Object take() throws InterruptedException {
    return buffer.take();
  }

  public Buffer put(Object item) throws InterruptedException {
    buffer.put(item);

    return this;
  }
}
