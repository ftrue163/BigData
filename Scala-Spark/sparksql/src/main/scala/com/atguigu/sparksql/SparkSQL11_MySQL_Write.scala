package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author layne
 */
object SparkSQL11_MySQL_Write {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0625\\input\\user.json")

    val df2: DataFrame = df.toDF("age2", "name2")

    //向mysql里写数据
    df2.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user3")
      .mode(SaveMode.Append)
      .save()


    //TODO 3 关闭资源
    spark.stop()
  }

}
