package com.atguigu.ex04_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 函数签名： def first(): T
 * 功能说明：返回RDD中的第一个元素
 */
object Test04_First {
    def main(args: Array[String]): Unit = {

        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)

        //3具体业务逻辑
        //3.1 创建第一个RDD
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        //3.2 返回RDD中元素的个数
        val firstResult: Int = rdd.first()
        println(firstResult)

        //4.关闭连接
        sc.stop()
    }
}
