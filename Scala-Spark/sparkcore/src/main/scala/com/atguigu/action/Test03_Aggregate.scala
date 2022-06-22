package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 14:18
 */
object Test03_Aggregate {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 1, 4, 1, 2, 7), 2)

    // 分区内逻辑
    // 10 + 1 + 2 + 3 = 16  10 + 4 + 5 + 6 + 7 = 32
    // 分区间逻辑
    // 10 - 16 - 32
    val i: Int = intRDD.aggregate(10)(_ + _, _ - _)
    println(i)


    // 分区内逻辑
    // 10 - 1 - 2 - 3 = 4  10-4-5-6-7=-12
    // 分区间逻辑
    // 10 - 4 - (-12) = 18
    val i1: Int = intRDD.fold(10)(_ - _)
    println(i1)


    // countByKey
    val tupleRDD: RDD[(Int, Int)] = intRDD.map((_, 1))
    val map: collection.Map[Int, Long] = tupleRDD.countByKey()

    println(map)

    // 4. 关闭sc
    sc.stop()
  }
}
