package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 9:19
 */
object Test04_GroupByKey {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[String] = sc.makeRDD(List("hello", "hello", "hello2", "hello1"), 2)

    val tupleRDD: RDD[(String, Int)] = intRDD.map((_, 1))

    // groupByKey
    // 最终分组的集合 只保留value
    val result: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
    result.collect().foreach(println)

    // 自动定义groupBy
    // 最终分组的集合  保留原数据
    val result1: RDD[(String, Iterable[(String, Int)])] = tupleRDD.groupBy(tuple => tuple._1)
    result1.collect().foreach(println)



    // 4. 关闭sc
    sc.stop()
  }
}
