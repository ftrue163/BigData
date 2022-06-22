package com.atguigu.createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-23 14:07
 */
object Test01_FromList {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 3. 从集合中创建rdd
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    rdd1.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()

  }
}
