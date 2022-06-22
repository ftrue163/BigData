package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 10:23
 */
object Test07_GroupByWc {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val strRDD: RDD[String] = sc.makeRDD(List("hello world hello scala", "hello spark"))


    // 拆分字符串
    val wordRDD: RDD[String] = strRDD.flatMap(_.split(" "))

    wordRDD.collect().foreach(println)

    // 映射为 (word,1)
    val tupleRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    tupleRDD.collect().foreach(println)


    // 分组聚合
    val wordToListRDD: RDD[(String, Iterable[(String, Int)])] = tupleRDD.groupBy((tuple: (String, Int)) => tuple._1)

    wordToListRDD.collect().foreach(println)

    // 算出次数
    val result: RDD[(String, Int)] = wordToListRDD.map((tuple: (String, Iterable[(String, Int)])) => (tuple._1, tuple._2.size))

    result.collect().foreach(println)



    // 可以不将字符串转换为元组  直接进行聚合
    val groupByRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(s => s)

    val result1: RDD[(String, Int)] = groupByRDD
      .map(tuple => (tuple._1, tuple._2.size))


    // 原rdd的数据类型是一个二元组
    // 在匿名函数中添加模式匹配
    val result2: RDD[(String, Int)] = groupByRDD.map( tuple => tuple match {
      case (word, list) => (word, list.size)
    })

    // 使用偏函数代替匿名函数
    val result3: RDD[(String, Int)] = groupByRDD.map({
      case (word, list) => (word, list.size)
    })






    // 4. 关闭sc
    sc.stop()
  }
}
