package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 *
 维度数据删除问题的考虑：
     1.先删除Redis,再更新Phoenix：
     2.先更新Phoenix,再删除Redis：
     3.延迟双删：先删除Redis,再更新Phoenix,最后再删除一次Redis

     最优方案：先写入Redis,再更新Phoenix
     为了简单且方案1基本满足需求  所以采用了方案1
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //connection.setAutoCommit(true);
    }

    //value:{"database":"", "tableName":"base_trademark", "before":{}, "after":{"id":"", ....}, "type":""}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        String sinkTable = value.getString("sinkTable");
        JSONObject after = value.getJSONObject("after");

        try {
            //每来一条数据  构建一次插入sql语句
            String upsertSQL = generateUpsertSQL(value);
            //测试：检查sql语句
            //System.out.println(upsertSQL);

            //编译SQL
            preparedStatement = connection.prepareStatement(upsertSQL);

            //如果为更新数据，则先删除Redis中的数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.deleteDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }

            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("写入Phoenix表数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    //value:{"database":"", "tableName":"base_trademark", "before":{}, "after":{"id":"", ....}, "type":""}
    //sql: upsert into database.tableName (column1, column2, ...) values ('value1', 'value1', ...)
    private String generateUpsertSQL(JSONObject value) {
        JSONObject after = value.getJSONObject("after");
        Set<String> keySet = after.keySet();
        Collection<Object> values = after.values();
        String columns = StringUtils.join(keySet, ',');
        String columnValues = StringUtils.join(values, "', '");

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + value.getString("sinkTable") + " (" + columns + ") values ('" + columnValues + "')";
    }
}
