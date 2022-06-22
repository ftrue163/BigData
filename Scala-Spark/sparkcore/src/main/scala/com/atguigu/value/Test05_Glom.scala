package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 10:02
 */
object Test05_Glom {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 0 -> 1,2   1 -> 3,4,5
    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    val arrayRDD: RDD[Array[Int]] = intRDD.glom()

    val listRDD: RDD[List[Int]] = arrayRDD.map(_.toList)

    listRDD.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)


    // 将文件中的多行数据 合并为rdd个数的单个字符串
    val sqlRDD: RDD[String] = sc.textFile("input/sql.txt",1)

    val value: RDD[Array[String]] = sqlRDD.glom()
    val value1: RDD[String] = value.map(array => array.mkString("\n"))

    // 4. 关闭sc
    sc.stop()
  }
}
