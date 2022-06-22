package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 9:28
 */
object Test04_FlatMap {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val listRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9)))

    listRDD.collect().foreach(println)

    // 扁平化操作
    val intRDD: RDD[Int] = listRDD.flatMap(list => list)

    intRDD.collect().foreach(println)

    // 将元素转换为 ("我是",int)
    val tupleRDD: RDD[(String, Int)] = intRDD.map(("我是", _))

    tupleRDD.collect().foreach(println)

    // 将两步操作合并在一起
    listRDD.flatMap(_.map(("我是",_)))


    // 切分长字符串
    val strRDD: RDD[String] = sc.makeRDD(List("hello world", "hello spark"))

    val wordRDD: RDD[String] = strRDD.flatMap(_.split(" "))

    wordRDD.collect().foreach(println)


    // 4. 关闭sc
    sc.stop()
  }
}
