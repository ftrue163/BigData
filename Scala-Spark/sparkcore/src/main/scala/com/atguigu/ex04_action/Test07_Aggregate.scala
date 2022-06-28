package com.atguigu.ex04_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



/**
 * 函数签名：def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
 * 函数说明：aggregate函数将每个分区里面的元素通过分区内逻辑和初始值进行聚合，然后用分区间逻辑和初始值（zeroValue）进行操作。注意：分区间逻辑再次使用初始值和aggregateByKey是有区别的。
 */
object Test07_Aggregate {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)

        val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

        // 分区内逻辑
        // 10 + 1 + 2 + 3 = 16  10 + 4 + 5 + 6 + 7 = 32
        // 分区间逻辑
        // 10 - 16 - 32 = -38
        val i: Int = intRDD.aggregate(10)(_ + _, _ - _)
        println(i)

        // 4.关闭sc
        sc.stop()
    }
}
