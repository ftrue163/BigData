package com.atguigu.ReadAndSave

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-27 18:16
 */
object Test03_ObjectFile {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val personRDD: RDD[Person] = sc.makeRDD(List(new Person("zhangsan", 10), new Person("lisi", 11)))

    // 作为对象文件保存
//    personRDD.saveAsObjectFile("objectFile")


    // 读取对象文件
    val value: RDD[Person] = sc.objectFile[Person]("objectFile")

    value.collect().foreach(println)


    // 4. 关闭sc
    sc.stop()
  }

  case class Person(name:String,age:Int)
}
