package com.atguigu.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class ThickClient {
    public static void main(String[] args) throws SQLException {
        // 胖客户端连接phoenix不需要额外启动服务
        // 直接连接zk即可
        // 2.创建配置
        Properties properties = new Properties();
        // 3.添加配置
        // 需要客户端服务端参数保存一致
        // 是否开启命名空间的映射  现在已经不再使用了
//        properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");

        // jdbc连接的四要素
        // url,驱动,user,password
        Connection connection = DriverManager.getConnection("jdbc:phoenix:hadoop102:2181");

        // 编译sql
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");

        // 执行sql
        ResultSet resultSet = preparedStatement.executeQuery();

        // 处理数据
        while (resultSet.next()){
            System.out.println(resultSet.getString(1) + "-" + resultSet.getString(2) + "-" + resultSet.getString(3));
        }

        // 关闭jdbc
        connection.close();

    }
}
