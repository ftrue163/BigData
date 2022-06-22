package com.atguigu.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-27 19:06
 */
object Test02_AvgAcc {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    // 创建自定义的累加器
    val acc = new MyAcc
    sc.register(acc)

    // 累加器必须使用在行动算子中
    intRDD.foreach(i => {
      acc.add(i)
    })

    // 获取累加器的value
    val tuple: (Int, Int) = acc.value

    println(tuple._1.toDouble / tuple._2)

    // 4. 关闭sc
    sc.stop()
  }

  // 自定义累加器
  // 求平均值
  class MyAcc extends AccumulatorV2[Int, (Int, Int)] {
    var sum = 0
    var count = 0

    // 判断累加器是否为空
    override def isZero: Boolean = sum == 0 && count == 0

    // 发送相同的累加器给executor
    override def copy(): AccumulatorV2[Int, (Int, Int)] = new MyAcc

    // 重置累加器
    override def reset(): Unit = {
      sum = 0
      count = 0
    }


    // 分区内累加
    override def add(v: Int): Unit = {
      sum += v
      count += 1
    }

    // 分区间合并累加器
    override def merge(other: AccumulatorV2[Int, (Int, Int)]): Unit = {
      sum += other.value._1
      count += other.value._2
    }

    // 返回累加器的结果
    override def value: (Int, Int) = (sum,count)
  }

}
