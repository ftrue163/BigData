package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 10:32
 */
object Test09_MapValues {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
          List(("a", 1), ("a", 3), ("a", 5), ("b", 7), ("b", 2), ("b", 4), ("b", 6), ("a", 7)), 2)

    rdd.map((tuple:(String,Int)) => (tuple._1,tuple._2 * 2))

    // 如果使用map的时候 二元组的key保持不变  只需要对value进行修改
    rdd.mapValues(i => i * 2)

    // 4. 关闭sc
    sc.stop()
  }
}
