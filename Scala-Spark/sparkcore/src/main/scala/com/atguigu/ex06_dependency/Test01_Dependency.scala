package com.atguigu.ex06_dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 16:02
 */
object Test01_Dependency {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/1.txt")
    println(lineRDD.toDebugString)
    println("------------------------------")

    val flatRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    println(flatRDD.toDebugString)
    println("------------------------------")

    val tupleRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    println(tupleRDD.toDebugString)
    println("------------------------------")

    val result: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    println(result.toDebugString)
    println("------------------------------")

//    result.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
