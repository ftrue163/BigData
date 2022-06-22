package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author layne
 */
object SparkSQL04_DataFrameAndDataSet {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //todo DF=>DS
    //创建DF
    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0625\\input\\user.json")

    df.show()

    import  spark.implicits._

    val ds: Dataset[User] = df.as[User]

    ds.show()

    //todo DS=>DF

    val df2: DataFrame = ds.toDF()

    df2.show()



    //TODO 3 关闭资源
    spark.stop()
  }

}
