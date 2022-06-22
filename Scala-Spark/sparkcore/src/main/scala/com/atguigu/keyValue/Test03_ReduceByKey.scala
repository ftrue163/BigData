package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 15:36
 */
object Test03_ReduceByKey {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2),("a",2),("b",4),("a",3),("b",6)),2)

    val value: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

//    value.mapPartitionsWithIndex((num,list) => list.map((num,_)))
//          .collect().foreach(println)

    val value1: RDD[(String, Int)] = rdd.reduceByKey((res:Int,elem:Int) => res - elem)
    value1.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)




    // 4. 关闭sc
    sc.stop()
  }
}
