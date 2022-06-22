package com.atguigu.ReadAndSave

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-27 18:09
 */
object Test02_SequenceFile {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    // key value类型的rdd
    val tupleRDD: RDD[(Int, Int)] = intRDD.map((_, 1))

    //    tupleRDD.saveAsSequenceFile("sequenceFile")

    // 使用相同的解析格式去读取对应的文件
    val tupleRDD1: RDD[(Int, Int)] = sc.sequenceFile[Int, Int]("sequenceFile")

    tupleRDD1.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
