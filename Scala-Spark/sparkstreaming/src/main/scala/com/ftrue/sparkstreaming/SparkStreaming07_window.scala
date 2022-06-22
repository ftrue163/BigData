package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author layne
 */
object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //1.对接数据源
    val lineDStrem: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //2 按照空格切分数据,并且炸开
    val wordDStream: DStream[String] = lineDStrem.flatMap(_.split(" "))

    //3 转换数据结构 word=>(word,1)
    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //4.对数据流进行开窗,窗口12s,滑动步长6s
    val word2oneDStreamByWindow: DStream[(String, Int)] = word2oneDStream.window(Seconds(12), Seconds(6))

    //5.求和
    val resultDStream: DStream[(String, Int)] = word2oneDStreamByWindow.reduceByKey(_ + _)

    //6 打印
    resultDStream.print()


    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

}
