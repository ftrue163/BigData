package com.atguigu.ex04_action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


/**
 * 函数签名：def fold(zeroValue: T)(op: (T, T) => T): T
 * 函数说明：折叠操作，aggregate的简化操作，即分区内逻辑和分区间逻辑相同。
 */
object Test08_Fold {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)

        //3具体业务逻辑
        //3.1 创建第一个RDD
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        //3.2 将该RDD所有元素相加得到结果
        //val foldResult: Int = rdd.fold(0)(_ + _)
        val foldResult: Int = rdd.fold(10)(_ + _)
        println(foldResult)

        //4.关闭连接
        sc.stop()
    }
}
