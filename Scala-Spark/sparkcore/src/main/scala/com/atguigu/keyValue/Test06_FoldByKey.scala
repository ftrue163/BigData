package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 9:37
 */
object Test06_FoldByKey {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 1), ("a", 3), ("a", 5), ("b", 7), ("b", 2), ("b", 4), ("b", 6), ("a", 7)), 2)


    // foldByKey
    // 可以使用初始值  分区内逻辑和分区间逻辑相同
    // 初始值会影响到每一个分区的计算  即每一个分区都会有一个初始值相加
    val result: RDD[(String, Int)] = rdd.foldByKey(10)(_ + _)

    result.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)



    // 4. 关闭sc
    sc.stop()
  }
}
