package com.atguigu.ex07_cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-27 10:29
 */
object Test02_CheckPoint {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")


    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("ck")

    val lineRDD: RDD[String] = sc.makeRDD(List("hello world", "hello scala"), 2)

    val lineRDD1: RDD[String] = lineRDD.map(s => {
      println("*********************")
      s + System.currentTimeMillis()
    })

    val wordRDD: RDD[String] = lineRDD1.flatMap(_.split(" "))

    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))


    // 检查点之前的血缘关系
    println(wordToOneRDD.toDebugString)
    println("----------------------------")

    // 添加检查点
    // 永久化保存
    // 需要手动添加路径
    // 检查点的数据 不会使用行动算子第一次计算的数据  而是在调用检查点的时候重写计算一次

    // 实际使用的情况下  在ck之前进行缓存  避免重复计算
    wordToOneRDD.cache()
    wordToOneRDD.checkpoint()


    // 第一次调用行动算子不会使用ck
    wordToOneRDD.collect().foreach(println)


    // 检查点之后血缘关系
    println(wordToOneRDD.toDebugString)
    println("----------------------------")

    // 后续的行动算子会使用ck保存的数据
    wordToOneRDD.collect().foreach(println)
    wordToOneRDD.collect().foreach(println)


    Thread.sleep(600000)

    // 4. 关闭sc
    sc.stop()
  }
}
