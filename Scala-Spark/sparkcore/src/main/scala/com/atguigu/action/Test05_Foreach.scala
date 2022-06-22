package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 14:33
 */
object Test05_Foreach {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(1 to 100, 10)

    // 收集数据为数组  绝对有序
//    intRDD.collect().foreach(println)

    // 调用行动算子foreach
    intRDD.foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
