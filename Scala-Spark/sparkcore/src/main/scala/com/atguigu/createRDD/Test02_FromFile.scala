package com.atguigu.createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-23 14:18
 */
object Test02_FromFile {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 3. 从文件系统创建RDD
    val rdd: RDD[String] = sc.textFile("E:\\bigdate\\project\\sparkcore0625\\input")

    val rdd1: RDD[String] = sc.textFile("hdfs://hadoop102:8020/input")

    rdd1.collect().foreach(println)



    // 4. 关闭sc
    sc.stop()
  }
}
