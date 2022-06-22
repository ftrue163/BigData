package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * jdbc的通用查询工具类
 *  比如：Phoenix  MySQL等等
 */
public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception{
        //创建集合用于存放查询结果
        ArrayList<T> result = new ArrayList<>();

        //编译SQL语句
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //遍历resultSet，对每行数据封装为T对象，并将T对象添加至集合
        while (resultSet.next()) {
            //通过反射的方式创建对象
            T t = clz.newInstance();

            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                BeanUtils.setProperty(t, columnName, value);
            }

            result.add(t);
        }

        resultSet.close();
        preparedStatement.close();

        //返回结果
        return result;
    }

    /**
     * 测试JdbcUtil工具类
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        /*System.out.println(queryList(connection, "select * from GMALL210625_REALTIME.DIM_BASE_TRADEMARK where id = '41'", JSONObject.class, true));*/
        System.out.println(queryList(connection, "select * from GMALL210625_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, true));

        connection.close();
    }
}
