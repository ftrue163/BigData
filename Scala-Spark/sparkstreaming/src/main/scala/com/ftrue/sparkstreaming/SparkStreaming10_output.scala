package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author layne
 */
object SparkStreaming10_output {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //需求:对接hadoop102的9999端口,获取数据源,做一个流式的WordCount
    //1.对接数据源
    val lineDStrem: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //2 按照空格切分数据,并且炸开
    val wordDStream: DStream[String] = lineDStrem.flatMap(_.split(" "))

    //3 转换数据结构 word=>(word,1)
    val word2oneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //4 按照key聚合value,求出每个单词的出现次数
    val resultDStream: DStream[(String, Int)] = word2oneDStream.reduceByKey(_ + _)

    //Driver端  全局执行一次
    println("111111111:" + Thread.currentThread().getName)

    //5 打印当前数据流
    resultDStream.foreachRDD(
      rdd => {
        //Driver端  一个批次执行一次
        println("222222:" + Thread.currentThread().getName)
        //将当前批次的数据收集打印
        println("------------------------")
        rdd.collect().foreach(
          x => println{
            x
          }
        )

/*        //把当前批次的数据存储到mysql中

        //创建mysql连接对象  //driver

        rdd.foreachPartition(

          //创建mysql连接对象  //executor
          iter => {
            iter.foreach(
              //executor
            )
          }
       */



      }

    )



    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

}
