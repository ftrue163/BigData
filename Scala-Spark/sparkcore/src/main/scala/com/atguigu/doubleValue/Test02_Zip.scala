package com.atguigu.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 15:11
 */
object Test02_Zip {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val intRDD1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // 拉链只能用于相同的分区相同的元素个数的rdd
    val value: RDD[(Int, Int)] = intRDD.zip(intRDD1)
    value.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)


    // 4. 关闭sc
    sc.stop()
  }
}
