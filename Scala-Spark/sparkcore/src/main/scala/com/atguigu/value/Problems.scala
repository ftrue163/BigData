package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 9:20
 */
object Problems {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val value: RDD[String] = sc.makeRDD(List("hello", "spark", "hello", "spark", "scala"))

    // 步骤一: 转换字符串为首字母
    val mapRDD: RDD[Char] = value.map(s => s.charAt(0))

    mapRDD.collect().foreach(println)

    val result: RDD[(Char, Int)] = mapRDD.map((_, 1)).reduceByKey(_ + _)

    result.collect().foreach(println)

    val result1: RDD[(Char, Int)] = value.map(s => (s.charAt(0), 1)).reduceByKey(_ + _)

    result1.collect().foreach(println)


    // 将分区的数据转换为一个string
    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    val value1: RDD[Array[Int]] = intRDD.glom()
    val value2: RDD[String] = value1.map(array => array.mkString(""))
    value2.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
