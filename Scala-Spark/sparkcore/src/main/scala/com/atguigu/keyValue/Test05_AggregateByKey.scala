package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 9:28
 */
object Test05_AggregateByKey {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 1), ("a", 3), ("a", 5), ("b", 7), ("b", 2), ("b", 4), ("b", 6), ("a", 7)), 2)

    // 需求: 求出每个分区的最大值  之后相加
    val result: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)((res, elem) => if (res > elem) res else elem, _ + _)

    result.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
