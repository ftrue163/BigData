package com.atguigu.ex04_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 1）saveAsTextFile(path)保存成Text文件
 * 功能说明：将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本
 * 2）saveAsSequenceFile(path)保存成Sequencefile文件
 * 功能说明：将数据集中的元素以Hadoop Sequencefile的格式保存到指定的目录下，可以使用HDFS或者其他Hadoop支持的文件系统。
 * 注意：只有kv类型RDD有该操作，单值的没有
 * 3）saveAsObjectFile(path)序列化成对象保存到文件
 * 功能说明：用于将RDD中的元素序列化成对象，存储到文件中。
 */
object Test10_Save {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf并设置App名称
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

        //2.创建SparkContext，该对象是提交Spark App的入口
        val sc: SparkContext = new SparkContext(conf)

        //3具体业务逻辑
        //3.1 创建第一个RDD
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        //3.2 保存成Text文件
        rdd.saveAsTextFile("output")

        //3.3 序列化成对象保存到文件
        rdd.saveAsObjectFile("output1")

        //3.4 保存成Sequencefile文件
        rdd.map((_, 1)).saveAsSequenceFile("output2")

        //4.关闭连接
        sc.stop()
    }
}
