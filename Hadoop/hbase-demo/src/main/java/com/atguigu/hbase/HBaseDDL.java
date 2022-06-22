package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * hbase ddl操作
 *   1.创建命名空间
 *   2.判断表是否存在
 *   3.创建表
 *   4.修改表
 *   5.删除表
 */
public class HBaseDDL {
    // 使用静态属性获取连接  之后在使用到连接的时候  不要去创建 而是直接调用这里的连接
    public static Connection connection = null;

    static {
        // 1. 创建配置对象
        Configuration conf = new Configuration();

        // 2. 添加配置
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        // 3. 创建hbase的连接
        // 需要使用ConnectionFactory来实例化
        try {
            // 一定要使用外部的属性进行赋值
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 单独写一个静态方法关闭
    public static void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }


    /**
     * 创建命名空间
     * @param nameSpace  命名空间的名称
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        // 获取admin
        Admin admin = connection.getAdmin();

        // 创建命名空间的描述
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);

        // 对应命名行方法create_namespace 'ns1', {'PROPERTY_NAME'=>'PROPERTY_VALUE'}
        builder.addConfiguration("user", "atguigu");

        NamespaceDescriptor descriptor = builder.build();

        // 调用方法创建命名空间
        try {
            admin.createNamespace(descriptor);
        } catch (IOException e) {
            System.out.println("命名空间已经存在");
        }

        // 关闭资源
        // 不要对admin连接进行缓存和池化  使用的时候在一个方法里面获取和关闭即可
        admin.close();
    }

    /**
     * 判断表格是否存在
     * @param nameSpace
     * @param table
     * @return
     * @throws IOException
     */
    public static boolean isTableExists(String nameSpace, String table) throws IOException {
        // 获取admin
        Admin admin = connection.getAdmin();

        // 调用方法判断表格是否存在
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(nameSpace, table));
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();

        // 返回对象
        return b;


    }


    /**
     * 创建表格
     * @param nameSpace
     * @param table
     * @param families
     * @throws IOException
     */
    public static void createTable(String nameSpace, String table, String... families) throws IOException {

        // 手动添加异常判断
        // 列族最少有一个
        if (families.length < 1) {
            System.out.println("请至少输入一个列族");
            return;
        }

        // 如果表格已经存在  也不能创建表格
        if (isTableExists(nameSpace, table)) {
            System.out.println("表格已经存在 无法创建");
            return;
        }

        // 获取admin
        Admin admin = connection.getAdmin();


        // 获取表格描述 建造者
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, table));

        // 添加多个列族描述
        for (String family : families) {
            // 创建列族描述 建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));

            // 修改列族的参数  对应create 'ns1:t1', {NAME => 'f1', VERSIONS => 5}
            columnFamilyDescriptorBuilder.setMaxVersions(5);

            // 添加列族信息
            // 创建列族描述 添加进表格描述中
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }


        // 调用方法创建表格
        // 使用表格描述 建造者创建表格描述
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭admin
        admin.close();


    }


    /**
     * 修改表格  对应一个列族的版本号
     * @param nameSpace
     * @param table
     * @param family
     * @param version
     * @throws IOException
     */
    public static void alterTable(String nameSpace, String table, String family, int version) throws IOException {

        // 判断表格是否存在
        if (!isTableExists(nameSpace, table)) {
            System.out.println("表格不存在  无法修改");
            return;
        }

        // 获取admin
        Admin admin = connection.getAdmin();

        // 工厂模式修改表格信息的时候 一定要调用原先的表格进行修改  否则会把所有信息全部恢复默认

        // 使用admin拿到老的表格描述
        TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(nameSpace, table));

        // 建造者模式创建的对象一般不能修改 只能读
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);

        // 调用老的列族描述
        ColumnFamilyDescriptor columnFamily = descriptor.getColumnFamily(Bytes.toBytes(family));

        // 创建对应的建造者 对版本号进行修改
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily);

        columnFamilyDescriptorBuilder.setMaxVersions(version);

        // 使用原先的描述 给到建造者 之后用建造者修改
        tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());

        // 使用admin修改表格
        try {
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }


//        // 创建表格描述
//        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, table));
//
//        // 创建列族的描述  用于修改
//        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
//
//        columnFamilyDescriptorBuilder.setMaxVersions(version);
//
//        // 修改列族的信息
//        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
//
//        // 调用方法修改表
//        try {
//            admin.modifyTable(tableDescriptorBuilder.build());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        // 关闭admin
        admin.close();
    }


    /**
     * 删除表格
     * @param nameSpace
     * @param table
     * @throws IOException
     */
    public static void dropTable(String nameSpace, String table) throws IOException {
        // 判断表格是否存在
        if (!isTableExists(nameSpace, table)) {
            System.out.println("表格不存在  无法删除");
        }

        // 获取admin
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        // 调用方法删除表格
        try {
            // 想要删除表格  需要先将其标记为disable
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭admin
        admin.close();

    }

    // hbase的连接推荐使用单例
    public static void main(String[] args) throws IOException {
        //测试创建命名空间
//        createNameSpace("bigdata1");
//
//        // 测试判断表示是否存在
//        System.out.println(isTableExists("default", "student"));
//
//
        // 测试创建表格
//        createTable("bigdata1","student1","info","info1");

        // 测试修改表格
//        alterTable("bigdata1","student1","info",9);

        // 测试删除表格
        dropTable("bigdata1", "student");

        System.out.println("别的代码");

        // 关闭连接
        closeConnection();
    }
}
