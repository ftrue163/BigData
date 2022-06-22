package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 10:12
 */
object Test06_GroupBy {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    // 0 -> 1,2,3  1 -> 4,5,6
    // 0 -> 1,2   1 -> 3,4  2 -> 5,6
    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),3)

    val tupleRDD: RDD[(Int, Iterable[Int])] = intRDD.groupBy(i => i % 2,4)

    tupleRDD.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    tupleRDD.saveAsTextFile("groupBy")

    Thread.sleep(300000)

    // 4. 关闭sc
    sc.stop()
  }
}
