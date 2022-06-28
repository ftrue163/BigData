package com.atguigu.ex04_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 函数签名：def countByKey(): Map[K, Long]
 * 功能说明：统计每种key的个数
 */
object Test09_CountByKey {
    def main(args: Array[String]): Unit = {

        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)

        //3具体业务逻辑
        //3.1 创建第一个RDD
        val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))

        //3.2 统计每种key的个数
        val result: collection.Map[Int, Long] = rdd.countByKey()
        println(result)

        //4.关闭连接
        sc.stop()
    }
}