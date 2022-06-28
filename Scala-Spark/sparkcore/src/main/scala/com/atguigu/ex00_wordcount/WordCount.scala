package com.atguigu.ex00_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        // 1.创建SparkConf并设置App名称
        //val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
        val conf = new SparkConf().setAppName("WC")

        // 2.创建SparkContext，该对象是提交Spark App的入口
        val sc = new SparkContext(conf)

        /*// 3.读取指定位置文件
        val lineRDD: RDD[String] = sc.textFile("D:\\RepCode\\IdeaProjects\\BigData\\Scala-Spark\\sparkcore\\input\\*")

        // 4.读取的一行一行的数据分解成一个一个的单词（扁平化）
        val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))

        // 5.将数据转换结构
        val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))

        // 6.将转换结构后的数据进行聚合处理
        val wordToSumRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey((v1, v2) => v1 + v2)

        // 7.将统计结果采集到控制台打印
        val wordToCountArray: Array[(String, Int)] = wordToSumRDD.collect()
        wordToCountArray.foreach(println)*/

        // 一行搞定
        sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))

        // 8.关闭连接
        sc.stop()
    }
}
