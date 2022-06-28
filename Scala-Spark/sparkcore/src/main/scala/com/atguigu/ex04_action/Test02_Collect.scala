package com.atguigu.ex04_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 函数签名：def collect(): Array[T]
 * 功能说明：在驱动程序中，以数组Array的形式返回数据集的所有元素。
 */
object Test02_Collect {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)

        //3具体业务逻辑
        //3.1 创建第一个RDD
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        //3.2 收集数据到Driver
        rdd.collect().foreach(println)

        //4.关闭连接
        sc.stop()
    }
}
