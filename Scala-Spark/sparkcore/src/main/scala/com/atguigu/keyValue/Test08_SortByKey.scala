package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 10:29
 */
object Test08_SortByKey {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    // spark中的排序 是保存全局有序的
    // 默认升序  可以修改为降序
    val sortRDD: RDD[(Int, String)] = rdd.sortByKey(false)
    sortRDD.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }
}
