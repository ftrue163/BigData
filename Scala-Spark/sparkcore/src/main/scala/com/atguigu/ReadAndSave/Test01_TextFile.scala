package com.atguigu.ReadAndSave

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-27 18:01
 */
object Test01_TextFile {
  def main(args: Array[String]): Unit = {
    // 修改使用hdfs的用户名
    System.setProperty("HADOOP_USER_NAME","atguigu")

    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)
    // 保存至本地磁盘
//    intRDD.saveAsTextFile("textFile")

    val intRDD1: RDD[String] = sc.textFile("textFile")

    intRDD1.collect().foreach(println)


    // 保存到hdfs
//    intRDD1.saveAsTextFile("hdfs://hadoop102:8020/textFile")

    // 从hdfs上面读取数据
    val value: RDD[String] = sc.textFile("hdfs://hadoop102:8020/textFile")
    value.collect().foreach(println)


    // 4. 关闭sc
    sc.stop()
  }
}
