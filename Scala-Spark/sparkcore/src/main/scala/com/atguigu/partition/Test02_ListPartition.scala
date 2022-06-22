package com.atguigu.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-23 14:35
 */
object Test02_ListPartition {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 分区结果: 0->1   1->2   2->3,4
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)

    rdd.saveAsTextFile("output")

    // 4. 关闭sc
    sc.stop()
  }
}
