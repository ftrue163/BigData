package com.atguigu.ex04_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 函数签名： def take(num: Int): Array[T]
 * 功能说明：返回一个由RDD的前n个元素组成的数组
 */
object Test05_Take {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)

        //3具体业务逻辑
        //3.1 创建第一个RDD
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        //3.2 返回RDD中元素的个数
        val takeResult: Array[Int] = rdd.take(2)
        //println(takeResult)
        takeResult.foreach(println)

        //4.关闭连接
        sc.stop()
    }
}
