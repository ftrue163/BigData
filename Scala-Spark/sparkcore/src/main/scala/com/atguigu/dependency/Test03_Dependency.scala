package com.atguigu.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 16:02
 */
object Test03_Dependency {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/1.txt",2)


    val flatRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    // 1.0 shuffleMapStage
    //    最后一个rdd的分区数 1个分区  1 task
    val tupleRDD: RDD[(String, Int)] = flatRDD.coalesce(1).map((_, 1))

    // shuffle切一刀
    // 2.0 resultStage
    //    2分区  2task
    val result: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _,2)


    // 一个行动算子代表一个job
    result.collect().foreach(println)
    result.saveAsTextFile("output")


    Thread.sleep(600000)

    // 4. 关闭sc
    sc.stop()
  }
}
