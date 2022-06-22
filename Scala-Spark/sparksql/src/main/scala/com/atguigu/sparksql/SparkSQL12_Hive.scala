package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author layne
 */
object SparkSQL12_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")

    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    //连接hive
    spark.sql("show tables").show()
    spark.sql("select * from test").show()
//    spark.sql("create table test2(id int,name string)").show()
    spark.sql("insert into table test2 values(1,'zhangsan')").show()
    spark.sql("select * from test2").show()

    /**
     * 用idea写代码连接外部hive步骤
     * 1.pom文件导入三个依赖 spark_sql spark_hive mysql连接驱动
     * 2.把hive-site.xml 复制一份到项目类路径下
     * 3.修改代码,获取外部hive的支持 .enableHiveSupport()
     */


    //TODO 3 关闭资源
    spark.stop()
  }

}
