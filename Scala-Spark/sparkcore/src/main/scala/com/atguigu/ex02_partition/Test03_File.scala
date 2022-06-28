package com.atguigu.ex02_partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * RDD数据从文件中读取后创建时的分区规则：
 *    没有指定分区的数量时：为当前核数和2的最小值
 *    指定分区的数量时：为指定分区的数量或者指定分区的数量加1
 */
object Test03_File {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    //1）默认分区的数量：默认取值为当前核数和2的最小值
    val rdd: RDD[String] = sc.textFile("sparkcore\\input\\1.txt")

    //2）输入数据1-4，每行一个数字；输出：0=>{1、2} 1=>{3} 2=>{4} 3=>{空}
    //val rdd: RDD[String] = sc.textFile("sparkcore\\input\\3.txt", 3)

    //3）输入数据1-4，一共一行；输出：0=>{1234} 1=>{空} 2=>{空} 3=>{空}
    //val rdd: RDD[String] = sc.textFile("sparkcore\\input\\4.txt", 3)

    rdd.saveAsTextFile("sparkcore\\output\\partition")

    // 4. 关闭sc
    sc.stop()
  }
}
