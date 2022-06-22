package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-23 16:02
 */
object Test02_MapPartition {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    // 将RDD的元素变成 2倍
    // 内部调用的map是集合常用函数中的  不是算子
    val rdd1: RDD[Int] = rdd.mapPartitions(_.map(_ * 2))

    rdd1.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
