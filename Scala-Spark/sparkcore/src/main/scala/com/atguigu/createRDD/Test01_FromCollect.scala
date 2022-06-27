package com.atguigu.createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从集合中创建RDD，Spark主要提供了两种函数：parallelize和makeRDD
 */
object Test01_FromCollect {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 3. 使用parallelize()创建rdd
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8))
    rdd.foreach(println)
    println("分区数" + rdd.partitions.size)

    // 4. 使用makeRDD()创建rdd
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8))
    rdd1.foreach(println)
    println("分区数" + rdd1.partitions.size)

    // 5. 关闭sc
    sc.stop()

  }
}
