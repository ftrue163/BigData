package com.atguigu.utils;


import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 工具类：
 *  获取ClickHouse的SinkFunction (通过自带的JdbcSink Connector的方式)
 */
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        Class<?> clz = t.getClass();
                        //获取所有的属性名
                        Field[] declaredFields = clz.getDeclaredFields();

                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            //获取当前属性
                            Field field = declaredFields[i];
                            field.setAccessible(true);

                            //尝试获取字段上的注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);

                            if (annotation != null) {
                                offset++;
                                continue;
                            }

                            //通过反射的方式来获取当前属性值
                            Object value = field.get(t);

                            //给预编译SQL对象赋值
                            preparedStatement.setObject(i + 1 - offset, value);
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
