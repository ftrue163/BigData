package com.atguigu.createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 由外部存储系统的数据集创建RDD包括：本地的文件系统，还有所有Hadoop支持的数据集，比如HDFS、HBase等
 */
object Test02_FromFile {
    def main(args: Array[String]): Unit = {
        // 1. 创建spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[*]")

        // 2. 创建sparkContext
        val sc = new SparkContext(conf)

        // 3. 从文件系统创建RDD
        // 3.1 本地文件系统
        //val rdd: RDD[String] = sc.textFile("D:\\RepCode\\IdeaProjects\\BigData\\Scala-Spark\\sparkcore\\input\\1.txt")
        val rdd: RDD[String] = sc.textFile("D:\\RepCode\\IdeaProjects\\BigData\\Scala-Spark\\sparkcore\\input\\*")
        // 3.2 HDFS文件系统
        //val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/input")

        // 4. 打印
        rdd.foreach(println)

        // 5. 关闭sc
        sc.stop()
    }
}
