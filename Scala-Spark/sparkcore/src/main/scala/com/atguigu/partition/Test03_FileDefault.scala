package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-23 15:01
 */
object Test03_FileDefault {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 默认填写的分区数为  (2 和 核数的最小值) 一般都是 2
    val rdd: RDD[String] = sc.textFile("input/1.txt",2)

    rdd.saveAsTextFile("output")

    // 4. 关闭sc
    sc.stop()
  }
}
