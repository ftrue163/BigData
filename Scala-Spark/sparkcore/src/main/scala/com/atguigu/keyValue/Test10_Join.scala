package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 10:37
 */
object Test10_Join {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")),3)

    //3.2 创建第二个pairRDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)),3)


    // join等同于sql的内连接
    val value: RDD[(Int, (String, Int))] = rdd.join(rdd1)

    value.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    println("========================")
    // cogroup类似于sql的满外连接
    val value1: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd1)
    value1.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)



    // 4. 关闭sc
    sc.stop()
  }
}
