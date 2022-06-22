package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 14:26
 */
object Test13_SortBy {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(10, 22, 13, 4, 5, 60, 17), 2)

    // 走shuffle的
    // 能够保证分区间也是有序的  使用分区号来进行排序
    val sortRDD: RDD[Int] = intRDD.sortBy(i => i)

    sortRDD.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)


    val tupleRDD: RDD[(String, Int)] = sc.makeRDD(List(("hello", 10), ("world", 22), ("scala", 13), ("spark", 1)), 2)

    // 按照单词的首字母进行排序

    val value: RDD[(String, Int)] = tupleRDD.sortBy(tuple => tuple._1)

    value.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    val value1: RDD[(String, Int)] = tupleRDD.sortBy(_._2)
    value1.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
