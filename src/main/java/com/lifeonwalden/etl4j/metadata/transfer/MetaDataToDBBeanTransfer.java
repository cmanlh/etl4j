package com.lifeonwalden.etl4j.metadata.transfer;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.lang.model.element.Modifier;

import org.apache.commons.lang3.StringUtils;

import com.lifeonwalden.util.db.FetchMetaStatement;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeSpec.Builder;
import com.squareup.javapoet.WildcardTypeName;

public class MetaDataToDBBeanTransfer implements MetaDataTransfer {
  private String packageName;

  private String className;

  /**
   * if not pass className, will take the first column's table name as class name
   * 
   * @param packageName
   * @param className
   */
  public MetaDataToDBBeanTransfer(String packageName, String className) {
    this.packageName = packageName;
    this.className = className;
  }

  @Override
  public String transfer(ResultSetMetaData rsmd) throws SQLException {
    JavaFile javaFile = JavaFile.builder(packageName, build(rsmd)).build();
    StringBuilder output = new StringBuilder();
    try {
      javaFile.writeTo(output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return output.toString();
  }

  @Override
  public String transfer(Connection connection) throws SQLException {
    StringBuilder output = new StringBuilder();
    try {
      DatabaseMetaData dbmd = connection.getMetaData();
      ResultSet tableResultSet = dbmd.getTables(null, null, "%", null);
      String prdName = dbmd.getDatabaseProductName();
      String driverName = dbmd.getDriverName();
      while (tableResultSet.next()) {
        String tableType = tableResultSet.getString(4);
        if ("TABLE".equalsIgnoreCase(tableType) || "VIEW".equalsIgnoreCase(tableType)) {
          JavaFile.builder(packageName,
              build(connection.createStatement()
                  .executeQuery(FetchMetaStatement.fetch(tableResultSet.getString(3), prdName, driverName))
                  .getMetaData()))
              .build().writeTo(output);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return output.toString();
  }

  public void toClassFile(ResultSetMetaData rsmd, String location) throws SQLException {
    JavaFile javaFile = JavaFile.builder(packageName, build(rsmd)).build();
    try {
      javaFile.writeTo(new File(location));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void toClassFile(Connection connection, String location) throws SQLException {
    File outputLocation = new File(location);
    try {
      DatabaseMetaData dbmd = connection.getMetaData();
      ResultSet tableResultSet = dbmd.getTables(null, null, "%", null);
      String prdName = dbmd.getDatabaseProductName();
      String driverName = dbmd.getDriverName();
      while (tableResultSet.next()) {
        String tableType = tableResultSet.getString(4);
        if ("TABLE".equalsIgnoreCase(tableType) || "VIEW".equalsIgnoreCase(tableType)) {
          JavaFile.builder(packageName,
              build(connection.createStatement()
                  .executeQuery(FetchMetaStatement.fetch(tableResultSet.getString(3), prdName, driverName))
                  .getMetaData()))
              .build().writeTo(outputLocation);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private TypeSpec build(ResultSetMetaData rsmd) throws SQLException {
    ClassName _className =
        ClassName.get(packageName.trim(), StringUtils.isBlank(className) ? rsmd.getTableName(1) : className.trim());
    Builder beanBuilder = TypeSpec.classBuilder(_className).addModifiers(Modifier.PUBLIC)
        .addSuperinterface(ParameterizedTypeName.get(Map.class, String.class, Object.class))
        .addField(FieldSpec
            .builder(ParameterizedTypeName.get(Map.class, String.class, Object.class), "dataMap", Modifier.PRIVATE)
            .initializer("new $T<String,Object>()", HashMap.class).build());

    beanBuilder
        .addMethod(MethodSpec.methodBuilder("size").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(int.class).addStatement("return dataMap.size()").build())
        .addMethod(MethodSpec.methodBuilder("isEmpty").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(boolean.class).addStatement("return dataMap.isEmpty()").build())
        .addMethod(MethodSpec.methodBuilder("containsKey").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(boolean.class).addParameter(Object.class, "key")
            .addStatement("return dataMap.containsKey($L)", "key").build())
        .addMethod(MethodSpec.methodBuilder("containsValue").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(boolean.class).addParameter(Object.class, "key")
            .addStatement("return dataMap.containsValue($L)", "key").build())
        .addMethod(MethodSpec.methodBuilder("get").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(Object.class).addParameter(Object.class, "key").addStatement("return dataMap.get($L)", "key")
            .build())
        .addMethod(MethodSpec.methodBuilder("put").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(Object.class).addParameter(String.class, "key").addParameter(Object.class, "value")
            .addStatement("return dataMap.put($L, $L)", "key", "value").build())
        .addMethod(MethodSpec.methodBuilder("remove").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(Object.class).addParameter(Object.class, "key").addStatement("return dataMap.remove($L)", "key")
            .build())
        .addMethod(MethodSpec.methodBuilder("putAll").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(void.class)
            .addParameter(ParameterizedTypeName.get(ClassName.get(Map.class), WildcardTypeName.subtypeOf(String.class),
                WildcardTypeName.subtypeOf(Object.class)), "m")
            .addStatement("dataMap.putAll($L)", "m").build())
        .addMethod(MethodSpec.methodBuilder("clear").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(void.class).addStatement("dataMap.clear()").build())
        .addMethod(MethodSpec.methodBuilder("keySet").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(Set.class, String.class)).addStatement("return dataMap.keySet()")
            .build())
        .addMethod(MethodSpec.methodBuilder("values").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(Collection.class, Object.class)).addStatement("return dataMap.values()")
            .build())
        .addMethod(MethodSpec.methodBuilder("entrySet").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Set.class),
                ParameterizedTypeName.get(Map.Entry.class, String.class, Object.class)))
            .addStatement("return dataMap.entrySet()").build());

    int colCount = rsmd.getColumnCount();
    for (int i = 1; i <= colCount; i++) {
      String fieldName = rsmd.getColumnLabel(i);
      String methodSubfix = StringUtils.capitalize(fieldName);
      Class<?> fieldClass = null;
      try {
        fieldClass = Class.forName(rsmd.getColumnClassName(i));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      beanBuilder
          .addMethod(MethodSpec.methodBuilder(StringUtils.join("set", methodSubfix)).addModifiers(Modifier.PUBLIC)
              .returns(_className).addParameter(fieldClass, fieldName)
              .addStatement("dataMap.put($S,$L)", fieldName, fieldName).addStatement("return this").build())
          .addMethod(MethodSpec.methodBuilder(StringUtils.join("get", methodSubfix)).addModifiers(Modifier.PUBLIC)
              .returns(fieldClass).addStatement("return ($T)dataMap.get($S)", fieldClass, fieldName).build());
    }

    return beanBuilder.build();
  }
}
