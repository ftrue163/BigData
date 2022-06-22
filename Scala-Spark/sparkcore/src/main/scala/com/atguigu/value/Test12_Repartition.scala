package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 14:19
 */
object Test12_Repartition {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 3)

    // 本质底层调用的是走shuffle的coalesce
    val result: RDD[Int] = intRDD.repartition(2)

    result.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
