package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-23 16:14
 */
object Test03_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    // 映射分区带分区号
    val value: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((num, list) => list.map(i => (num, i * 2)))

    value.collect().foreach(println)

    rdd.mapPartitionsWithIndex((num,list) => list.map((num,_)))
      .collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
