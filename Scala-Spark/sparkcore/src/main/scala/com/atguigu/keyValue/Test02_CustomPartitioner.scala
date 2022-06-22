package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author yhm
 * @create 2021-09-24 15:27
 */
object Test02_CustomPartitioner {
  def main(args: Array[String]): Unit = {
    // 1. 创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    // 2. 创建sparkContext
    val sc = new SparkContext(conf)


    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    val tupleRDD: RDD[(Int, Int)] = intRDD.map((_, 1))

    val result: RDD[(Int, Int)] = tupleRDD.partitionBy(new MyPartitioner(3))

    result.mapPartitionsWithIndex((num,list) => list.map((num,_)))
          .collect().foreach(println)

    // 4. 关闭sc
    sc.stop()
  }


  class MyPartitioner (num:Int) extends Partitioner{

    override def numPartitions: Int = num

    // 按照设定的规则对数据进行分区
    // 只能按照key来分区
    // 如果key小于5 放入0号分区  如果大于等于5  放入1号分区
    // 返回的分区数值  必须小于总分区数
    override def getPartition(key: Any): Int = {
      key match {
        case i:Int => if (i < 5) 0 else 1
        case _ => 2
      }
    }
  }

}
