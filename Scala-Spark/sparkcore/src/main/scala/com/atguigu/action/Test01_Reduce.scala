package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 14:04
 */
object Test01_Reduce {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    // 行动算子reduce
    val i: Int = intRDD.reduce(_ - _)
    println(i)

    // 行动算子collect
    println(intRDD.collect().toList)

    // 4. 关闭sc
    sc.stop()
  }
}
