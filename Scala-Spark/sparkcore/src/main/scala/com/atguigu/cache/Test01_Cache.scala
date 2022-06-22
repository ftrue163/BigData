package com.atguigu.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-27 10:11
 */
object Test01_Cache {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.makeRDD(List("hello world", "hello scala"), 2)

    val lineRDD1: RDD[String] = lineRDD.map(s => {
      println("*********************")
      s
    })

    val wordRDD: RDD[String] = lineRDD1.flatMap(_.split(" "))

    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))


    // 使用缓存 来保存已经计算过一遍的数据  下次使用就不需要再次计算了
    //    wordToOneRDD.persist()
    //    wordToOneRDD.cache()

    // 缓存之前的血缘关系
    println(wordToOneRDD.toDebugString)
    println("---------------------------")

    wordToOneRDD.persist()

    wordToOneRDD.collect()

    // 缓存之后的血缘关系
    println(wordToOneRDD.toDebugString)
    println("---------------------------")

    // 使用reduceByKey
    // 如果有shuffle的话  自动会进行缓存
    val result: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_ + _)



//    result.collect().foreach(println)
//    result.collect().foreach(println)


    Thread.sleep(600000)

    // 4. 关闭sc
    sc.stop()
  }
}
