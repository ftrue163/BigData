package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 14:03
 */
object Test11_Coalesce {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)
    intRDD.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)
    // 缩减分区
    val result: RDD[Int] = intRDD.coalesce(2,true)

    println("============================")
    result.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    Thread.sleep(300000)

    // 4. 关闭sc
    sc.stop()
  }
}
