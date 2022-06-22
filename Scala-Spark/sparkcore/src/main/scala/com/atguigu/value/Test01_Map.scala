package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-23 15:28
 */
object Test01_Map {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),2)
//    rdd.saveAsTextFile("map1")

    val mapRdd: RDD[Int] = rdd.map((i) => i * 2)
    val mapRdd1: RDD[Int] = rdd.map( _ * 2)
//    mapRdd.saveAsTextFile("map2")

    mapRdd.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    mapRdd.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
