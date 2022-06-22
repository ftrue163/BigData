package com.atguigu.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author yhm
 * @create 2021-09-27 19:22
 */
object Test03_CustomAcc {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    //    需求：自定义累加器，统计RDD中首字母为“H”的单词以及出现的次数。
    val wordRDD: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hi", "Hi", "Spark", "Spark"))

    // 注册累加器
    val acc = new CustomAcc
    sc.register(acc)


    wordRDD.foreach(s => {
      acc.add(s)
    })

    // 获取累加器的值
    val map: mutable.Map[String, Int] = acc.value

    println(map)


    // 使用非累加器的方式完成需求
    val value: RDD[(String, Int)] = wordRDD.filter(_.startsWith("H"))
      .map((_, 1))
      .reduceByKey(_ + _)
    value.collect().foreach(println)


    Thread.sleep(6000000)

    // 4. 关闭sc
    sc.stop()
  }

  // 自定义累加器
  class CustomAcc extends AccumulatorV2[String, mutable.Map[String, Int]] {

    val map = mutable.Map[String, Int]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new CustomAcc

    override def reset(): Unit = map.clear()

    // 分区内使用map累加单词
    override def add(v: String): Unit = {
      // 判断单词是否是H开头
      if (v.startsWith("H")) {
        // 更新或添加map中的元素
//        if (map.contains(v)) {
//          map.put(v, map.getOrElse(v, 0) + 1)
//        }else{
//          map.put(v,1)
//        }
        // 使用伴生类的apply方法更加简单
        map.put(v, map.getOrElse(v, 0) + 1)
//        map(v) =  map.getOrElse(v, 0) + 1
      }

    }

    // 分区间合并两个map
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      // 合并map  需要将结果给到本地的map  不是other中的map1
      val map1: mutable.Map[String, Int] = other.value

      for (elem <- map1) {
        map.put(elem._1,map.getOrElse(elem._1,0) + elem._2)
      }

    }

    override def value: mutable.Map[String, Int] = map
  }

}
