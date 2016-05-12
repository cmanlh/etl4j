package com.lifeonwalden.etl4j.metadata.transfer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.lang.model.element.Modifier;

import org.apache.commons.lang3.StringUtils;

import com.lifeonwalden.util.db.FetchMetaStatement;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeSpec.Builder;

public class MetaDataToDBBeanTransfer implements MetaDataTransfer {
    private String packageName;

    private String className;

    /**
     * if not pass className, will take the first column's table name as class
     * name
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
                    JavaFile.builder(
                            packageName,
                            build(connection.createStatement()
                                    .executeQuery(FetchMetaStatement.fetch(tableResultSet.getString(3), prdName, driverName)).getMetaData())).build()
                            .writeTo(output);
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
                    JavaFile.builder(
                            packageName,
                            build(connection.createStatement()
                                    .executeQuery(FetchMetaStatement.fetch(tableResultSet.getString(3), prdName, driverName)).getMetaData())).build()
                            .writeTo(outputLocation);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private TypeSpec build(ResultSetMetaData rsmd) throws SQLException {

        assert StringUtils.isNotBlank(packageName);

        ClassName _className = ClassName.get(packageName.trim(), StringUtils.isBlank(className) ? rsmd.getTableName(1) : className.trim());
        Builder beanBuilder =
                TypeSpec.classBuilder(_className)
                        .addModifiers(Modifier.PUBLIC)
                        .addSuperinterface(Serializable.class)
                        .addField(
                                FieldSpec.builder(long.class, "serialVersionUID", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                                        .initializer("$L", System.currentTimeMillis()).build());
        int colCount = rsmd.getColumnCount();
        for (int i = 1; i <= colCount; i++) {
            String fieldName = rsmd.getColumnLabel(i);
            String methodSubfix = StringUtils.capitalize(rsmd.getColumnLabel(i));
            Class<?> fieldClass = null;
            try {
                fieldClass = Class.forName(rsmd.getColumnClassName(i));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            beanBuilder
                    .addField(FieldSpec.builder(fieldClass, fieldName, Modifier.PRIVATE).build())
                    .addMethod(
                            MethodSpec.methodBuilder(StringUtils.join("set", methodSubfix)).addModifiers(Modifier.PUBLIC).returns(_className)
                                    .addParameter(fieldClass, fieldName).addStatement("this.$L = $L", fieldName, fieldName)
                                    .addStatement("return this").build())
                    .addMethod(
                            MethodSpec.methodBuilder(StringUtils.join("get", methodSubfix)).addModifiers(Modifier.PUBLIC).returns(fieldClass)
                                    .addStatement("return this.$L", fieldName).build());
        }

        return beanBuilder.build();
    }
}
