package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author layne
 */
object SparkSQL09_Save {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")

    conf.set("spark.sql.sources.default","json")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //数据保存
    val df: DataFrame = spark.read.json("D:\\IdeaProjects\\SparkSQL0625\\input\\user.json")

    df.show()

    //特定保存
//    df.write.json("D:\\IdeaProjects\\SparkSQL0625\\outjson")

//    df.write.csv("D:\\IdeaProjects\\SparkSQL0625\\outcsv")

    //通用保存
    df.write.mode(SaveMode.Ignore).save("D:\\IdeaProjects\\SparkSQL0625\\out1")

    /**
     * 四种写出模式
     * 1.默认模式 存在即报错  如果目标路径不存在,正常写入,如果目标路径存在了,报错
     * 2.追加模式            如果目标路径不存在,正常写入,如果目标路径存在了,追加写入
     * 3.覆盖模式            如果目标路径不存在,正常写入,如果目标路径存在了,删除目录路径,重新创建一个新的同名路径,写入
     * 4.忽略模式            如果目标路径不存在,正常写入,如果目标路径存在了,忽略本次操作,啥也不干,不报错
     */


    //TODO 3 关闭资源
    spark.stop()
  }

}
