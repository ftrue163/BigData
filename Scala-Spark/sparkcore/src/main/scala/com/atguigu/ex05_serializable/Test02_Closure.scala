package com.atguigu.ex05_serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 *
 *
 */
object Test02_Closure {
    def main(args: Array[String]): Unit = {
        //1.创建spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[*]")

        //2.创建SparkContext
        val sc: SparkContext = new SparkContext(conf)

        val rdd1: RDD[String] = sc.makeRDD(List("hello", "scala", "spark", "hello world"))

        //3.1 打印，ERROR报java.io.NotSerializableException
        val searcher = new Searcher("hello")
        val rdd2 = searcher.myFilter(rdd1)
        //val rdd2 = searcher.myFilter2(rdd1)
        rdd2.foreach(println)

        //4.关闭sc
        sc.stop()
    }

}

class Searcher(query: String) {
//方式1：继承Serializable
//class Searcher(query: String) extends Serializable {
//方式2：样例类
//case class Searcher(query: String) {
    //一、属性需要序列化   [3种处理方式]
    //过滤出当前RDD中包含query的元素
    def myFilter(rdd: RDD[String]): RDD[String] = {
        //方式3：属性变为已序列化的数据类型
        val str: String = query
        rdd.filter(_.contains(str))

        //rdd.filter(_.contains(query))
    }


    //二、方法需要序列化   [2种处理方式]
    //过滤出当前RDD中包含query的元素
    def isContains(str: String): Boolean = {
        str.contains("hello")
    }
    def myFilter2(rdd: RDD[String]): RDD[String] = {
        rdd.filter(isContains)
    }
}