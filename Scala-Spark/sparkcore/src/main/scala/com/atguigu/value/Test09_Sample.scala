package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 11:12
 */
object Test09_Sample {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    // 抽取数据不放回（伯努利算法）
    // 伯努利算法：又叫0、1分布。例如扔硬币，要么正面，要么反面。
    // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
    // 第一个参数：抽取的数据是否放回，false：不放回
    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
    // 第三个参数：随机数种子
    val value: RDD[Int] = intRDD.sample(false, 0.5,100)

    // 保留原有的分区
    value.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)


    println("================================")
    // 抽取数据放回（泊松算法）
    // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
    // 第二个参数：重复数据的几率，范围大于等于0.表示每一个元素被期望抽取到的次数
    // 第三个参数：随机数种子

    val value1: RDD[Int] = intRDD.sample(true, 2,100)
    value1.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)



    // 4. 关闭sc
    sc.stop()
  }
}
