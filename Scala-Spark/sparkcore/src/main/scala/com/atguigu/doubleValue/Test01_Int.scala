package com.atguigu.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 15:02
 */
object Test01_Int {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val intRDD1: RDD[Int] = sc.makeRDD(List(5, 6, 7, 8, 9, 10), 3)

    // 求交集
    // 最终的分区个数使用的是较多的rdd的分区数
    val value: RDD[Int] = intRDD.intersection(intRDD1)

//    value.mapPartitionsWithIndex((num,list) => list.map((num,_)))
//          .collect().foreach(println)

//    value.saveAsTextFile("intersection")


    // 求并集
    // 数据不发生变化  也不去重  只是把多个分区的数据合在一起  结果的分区个数是rdd分区个数之和
    val value1: RDD[Int] = intRDD.union(intRDD1)
//    value1.mapPartitionsWithIndex((num,list) => list.map((num,_)))
//          .collect().foreach(println)


    // 求差集
    // 结果会打散重新分区  走shuffle
    val value2: RDD[Int] = intRDD.subtract(intRDD1)
    value2.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)



    // 4. 关闭sc
    sc.stop()
  }
}
