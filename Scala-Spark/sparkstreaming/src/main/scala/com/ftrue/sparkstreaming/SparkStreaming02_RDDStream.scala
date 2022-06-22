package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author layne
 */
object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(4))

    //创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()


    //利用RDD队列来创建DSteam
    // oneAtATime = true 默认，一次读取队列里面的一个数据
    // oneAtATime = false， 按照设定的批次时间，读取队列里面数据
    val rddDStream: InputDStream[Int] = ssc.queueStream(rddQueue,false)

    //累加rddDStream每个批次的和
    val resultDStream: DStream[Int] = rddDStream.reduce(_ + _)

    //打印
    resultDStream.print()



    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()

    //在启动ssc线程以后,立马往rdd队列里面放数据,2s放一个1 to 5的rdd
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()



  }

}
