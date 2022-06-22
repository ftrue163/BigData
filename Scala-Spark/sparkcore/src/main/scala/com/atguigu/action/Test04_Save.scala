package com.atguigu.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-25 14:26
 */
object Test04_Save {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    // 保存为txt文本
    // 可以看懂  hdfs文件的存储格式
    // 有几个分区最后就会形成集合文件
    //    intRDD.saveAsTextFile("textFile")

    // 保存二进制文件
    intRDD.map((_, 1))
    //        .saveAsSequenceFile("sequenceFile")

    // 保存对象文件
    intRDD.saveAsObjectFile("objectFile")

    // 4. 关闭sc
    sc.stop()
  }
}
