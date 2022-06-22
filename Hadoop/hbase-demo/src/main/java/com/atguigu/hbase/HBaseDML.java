package com.atguigu.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * hbase dml操作
 *   1.插入数据
 *   2.查询数据
 *   3.扫描数据
 *   4.删除数据
 */
public class HBaseDML {
    public static Connection connection = HBaseUtilConnection.connection;

    /**
     * 插入数据
     *
     * @param nameSpace 命名空间
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param column    列名
     * @param value     值
     * @throws IOException 连接有问题
     */
    public static void putCell(String nameSpace, String tableName, String rowKey, String family, String column, String value) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        // 创建一个put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

        // 调用方法put数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭table
        table.close();
    }


    /**
     * 读取数据
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void getCell(String nameSpace, String tableName, String rowKey, String family, String column) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        // 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 如果想要获取完整一行的数据 直接将创建的get对象放入到方法中即可
        // 如果想要获取某一列数据  也可以重复添加几列
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

        // 如果想要读多个版本的数据
        get.readVersions(7);

        // 调用相关方法获取数据
        Result result = table.get(get);


        // 处理读到的数据  get得到的数据就是cell数组
        Cell[] cells = result.rawCells();

//        for (Cell cell : cells) {
//            System.out.println(new String(CellUtil.cloneValue(cell)));
//        }

        // 类似于迭代器  advance 就是 hasNext  current就是next方法
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            System.out.println(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)));
        }


        // 最简单的处理方法  内部获取的值是byte数组
//        System.out.println(new String(result.value()));

        // 关闭table
        table.close();


    }


    /**
     * 扫描数据
     * @param nameSpace
     * @param tableName
     * @param startRowKey
     * @param stopRowKey
     * @throws IOException
     */
    public static void scanTable(String nameSpace, String tableName, String startRowKey, String stopRowKey) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        // 创建scan对象
        Scan scan = new Scan();

        // 填写开始的行和结束的行
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(stopRowKey));

        // 调用方法scan
        ResultScanner resultScanner = table.getScanner(scan);

        // 结果resultScanner是result的数组
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)) + "\t");
            }
            System.out.println();
        }

        // 关闭table
        table.close();

    }


    /**
     * 删除列数据
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void deleteColumn(String nameSpace, String tableName, String rowKey, String family, String column) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        // 创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // addColumn对应delete  删除一个版本
        // addColumns对应deleteAll  删除所有版本
//        delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));

        // 调用方法删除列
        table.delete(delete);

        // 关闭table
        table.close();
    }

    public static void main(String[] args) throws IOException {
        // 测试插入数据
//        putCell("bigdata1", "student1", "1001", "info", "name", "zhangsan2");

        // 测试删除数据
        //deleteColumn("bigdata1", "student1", "1001", "info", "name");

        //getCell("bigdata1", "student1", "1001", "info", "name");

        // 测试scan
//        scanTable("default","student","1001","1008");

        String rowKey = "aaa";

        String hex = Bytes.toHex(Bytes.toBytes(rowKey));

        System.out.println(hex);

        // 关闭连接
        //HBaseUtilConnection.close();
    }
}
