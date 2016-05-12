package com.lifeonwalden.etl4j.metadata;

import java.io.Serializable;

public class Column implements Serializable {
  private static final long serialVersionUID = -626049469653712296L;

  private String name;
  private int index;
  private Class<?> clazz;

  public String getName() {
    return name;
  }

  public Column setName(String name) {
    this.name = name;
    return this;
  }

  public int getIndex() {
    return index;
  }

  public Column setIndex(int index) {
    this.index = index;
    return this;
  }

  public Class<?> getClazz() {
    return clazz;
  }

  public Column setClazz(Class<?> clazz) {
    this.clazz = clazz;
    return this;
  }

}
