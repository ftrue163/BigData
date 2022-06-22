package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author layne
 */
object SparkSQL01_input {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取数据创建DF
    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0625\\input\\user.json")

    //SQL风格语法
    df.createTempView("user")
    spark.sql("select * from user where age=20").show()


    //DSL风格语法
    df.select("name","age").where("age >= 19").show()


    //TODO 3 关闭资源
    spark.stop()

  }
}
