package com.atguigu.ex02_partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD数据从集合中创建时的分区源码查看
 */
object Test02_CollectPartition {
    def main(args: Array[String]): Unit = {
        // 1. 创建spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[*]")

        // 2. 创建sparkContext
        val sc: SparkContext = new SparkContext(conf)

        //1）4个数据，设置4个分区，输出：0分区->1，1分区->2，2分区->3，3分区->4
        //val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 4)

        //2）4个数据，设置3个分区，输出：0分区->1，1分区->2，2分区->3,4
        //val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 3)

        //3）5个数据，设置3个分区，输出：0分区->1，1分区->2、3，2分区->4、5
        val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5), 3)

        rdd.saveAsTextFile("D:\\RepCode\\IdeaProjects\\BigData\\Scala-Spark\\sparkcore\\output\\partition\\")

        // 4. 关闭sc
        sc.stop()
    }
}
