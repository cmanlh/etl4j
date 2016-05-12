package com.lifeonwalden.etl4j.metadata;

import java.io.Serializable;

public class Param implements Serializable {
  private static final long serialVersionUID = 479511133442881479L;

  private int type;
  private int index;
  private Class<?> clazz;
  private int mode;

  public int getType() {
    return type;
  }

  public Param setType(int type) {
    this.type = type;
    return this;
  }

  public int getIndex() {
    return index;
  }

  public Param setIndex(int index) {
    this.index = index;
    return this;
  }

  public Class<?> getClazz() {
    return clazz;
  }

  public Param setClazz(Class<?> clazz) {
    this.clazz = clazz;
    return this;
  }

  public int getMode() {
    return mode;
  }

  public Param setMode(int mode) {
    this.mode = mode;
    return this;
  }

}
