package com.atguigu.phoenix;


import org.apache.phoenix.queryserver.client.ThinClientUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ThinClient {
    public static void main(String[] args) throws SQLException {
        // 瘦客户端需要连接queryServer  地址连接不同
        // 直接连接zk即可
        // jdbc连接的四要素
        // url,驱动,user,password
        String url = ThinClientUtil.getConnectionUrl("hadoop102",8765);
        System.out.println(url);

        Connection connection = DriverManager.getConnection(url);

        // 编译sql
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");

        // 执行sql
        ResultSet resultSet = preparedStatement.executeQuery();

        // 处理数据
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "-" + resultSet.getString(2) + "-" + resultSet.getString(3));
        }

        // 关闭jdbc
        connection.close();
    }
}
