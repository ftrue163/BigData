package com.atguigu.serializable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yhm
 * @create 2021-09-25 15:17
 */
object Test03_Serializable {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val search = new Search("atguigu")

    val bool: Boolean = search.isMatch("hello world atguigu")
    println(bool)

    val rdd: RDD[String] = sc.makeRDD(List("hello world", "hello scala", "hello atguigu"))

    //    val rdd1: RDD[String] = search.getMatch1(rdd)
    //    val rdd1: RDD[String] = search.getMatch2(rdd)

    val str: String = search.query

    val rdd1: RDD[String] = rdd.filter(x => x.contains(str))

    rdd1.collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }

  class Search(val query: String) {

    // 定义一个工具方法
    // 传入的字符串是否包含定义工具的关键字
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {

      rdd.filter(this.isMatch _)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      // 和rdd无关在driver端执行
      val str: String = query
      // 和rdd相关 在executor端执行 不过使用的是str不是match类
      rdd.filter(x => x.contains(str))

    }
  }

}
