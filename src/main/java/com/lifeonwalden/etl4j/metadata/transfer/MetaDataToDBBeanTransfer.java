package com.lifeonwalden.etl4j.metadata.transfer;

import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.lang.model.element.Modifier;

import org.apache.commons.lang3.StringUtils;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeSpec.Builder;

public class MetaDataToDBBeanTransfer implements MetaDataTransfer {
    private String packageName;

    private String className;

    public MetaDataToDBBeanTransfer(String packageName, String className) {
        this.packageName = packageName;
        this.className = className;
    }

    @Override
    public String transfer(ResultSetMetaData rsmd) throws SQLException {
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
        JavaFile javaFile = JavaFile.builder(packageName, beanBuilder.build()).build();
        try {
            javaFile.writeTo(System.out);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return "";
    }
}
