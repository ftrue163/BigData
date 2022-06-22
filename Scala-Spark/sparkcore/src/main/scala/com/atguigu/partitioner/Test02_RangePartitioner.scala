package com.atguigu.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-27 11:13
 */
object Test02_RangePartitioner {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.makeRDD(List("hello world"),2)

    // 只有key value类型的rdd才有分区器
    println(lineRDD.partitioner)

    // 即使的key value类型 如果没有放分区器 也为空
    val tupleRDD: RDD[(String, Int)] = lineRDD.map((_, 1))
    println(tupleRDD.partitioner)

    // 如果是分组聚合的话 使用的是hash分区器
//    val reduceRDD: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _,2)
//    println(reduceRDD.partitioner)
//
//    val groupRDD: RDD[(String, Iterable[Int])] = reduceRDD.groupByKey(2)

    // 如果使用排序 使用的是范围分区器
    val sortRDD: RDD[(String, Int)] = tupleRDD.sortByKey()
    println(sortRDD.partitioner)

    // 只使用一个行动算子
    sortRDD.collect()


    Thread.sleep(650000)


    // 4. 关闭sc
    sc.stop()
  }
}
