package com.atguigu.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-28 9:22
 */
object Test01_Broadcast {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.makeRDD(List("hello world", "hello scala","hello hive", "hello spark"),8)

    val list = List("scala", "spark")

    // 过滤出rdd中的数据 包含有list集合中任意一个单词的数据
    // 如果使用常规的变量方法, 需要发送task个数的list数据
    val result: RDD[String] = lineRDD.filter(s => {
      var flag = false
      for (elem <- list) {
        if (s.contains(elem))
          flag = true
      }
      flag
    })

    result.collect().foreach(println)


    // 使用广播变量
    val broadcast: Broadcast[List[String]] = sc.broadcast(list)

    // 使用广播变量发送的是executor个数的list
    val result1: RDD[String] = lineRDD.filter(s => {
      var flag = false
      // 调用广播变量
      for (elem <- broadcast.value) {
        if (s.contains(elem)) flag = true
      }
      flag
    })

    result1.collect().foreach(println)


    Thread.sleep(600000)
    // 4. 关闭sc
    sc.stop()
  }
}
