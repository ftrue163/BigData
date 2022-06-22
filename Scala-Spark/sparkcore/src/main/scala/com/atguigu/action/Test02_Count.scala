package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 14:10
 */
object Test02_Count {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    val counts: Long = intRDD.count()
    println(counts)

    val elem1: Int = intRDD.first()
    println(elem1)


    val array: Array[Int] = intRDD.take(4)
    println(array.toList)


    // 取rdd中最大的3个数
    val array1: Array[Int] = intRDD.sortBy(a => a, false).take(3)
    println(array1.toList)
    val array2: Array[Int] = intRDD.takeOrdered(3)(Ordering[Int].reverse)
    println(array2.toList)

    // 4. 关闭sc
    sc.stop()
  }
}
