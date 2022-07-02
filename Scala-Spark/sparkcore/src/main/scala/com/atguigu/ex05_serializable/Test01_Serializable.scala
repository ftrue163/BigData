package com.atguigu.ex05_serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 *  为什么序列化?
 *   -因为Spark程序初始化操作发生在Driver端，具体算子的执行是在Executor端执行的
 *   如果在Executor执行的时候，要访问Driver端初始化的数据，那么就涉及跨进程或者跨节点之间的通信，
 *   所以要求传递的数据必须是可序列化
 *
 *   如何确定是否序列化
 *   -在执行RDD相关算子之前，有一段这样的代码val cleanF = sc.clean(f)，判断是否进行了闭包检测
 *   -之所以叫闭包检测，因为算子也是一个函数，算子函数内部访问了外部函数的局部变量，形成了闭包
 *
 *
 */
object Test01_Serializable {
    def main(args: Array[String]): Unit = {
        //1.创建spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[*]")

        //2.创建SparkContext
        val sc: SparkContext = new SparkContext(conf)

        //3.创建两个对象
        val user1 = new User()
        user1.name = "zhangsan"

        val user2 = new User()
        user2.name = "lisi"

        val userRDD1: RDD[User] = sc.makeRDD(List(user1, user2))

        //3 如果User类没有序列化，则报错：java.io.NotSerializableException
        userRDD1.foreach(println)

        //4.关闭sc
        sc.stop()
    }


    class User() {
        var name: String = _
        override def toString: String = s"User($name)"
    }


    /*class User extends Serializable {
        var name: String = _
        override def toString: String = s"User($name)"
    }*/
}
