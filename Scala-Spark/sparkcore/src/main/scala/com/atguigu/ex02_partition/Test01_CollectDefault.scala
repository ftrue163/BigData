package com.atguigu.ex02_partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * RDD数据从集合中创建时的分区规则：
 *    没有指定分区个数时，分区个数和核数一样
 *    指定分区个数时，分区个数则为指定个数
 */
object Test01_CollectDefault {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[8]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 3. 默认使用环境的核数(local[*]使用的是计算机的核数) 可以手动填写分区数
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8),2)

    rdd.saveAsTextFile("D:\\RepCode\\IdeaProjects\\BigData\\Scala-Spark\\sparkcore\\output\\partition\\")

    // 4. 关闭sc
    sc.stop()
  }
}
