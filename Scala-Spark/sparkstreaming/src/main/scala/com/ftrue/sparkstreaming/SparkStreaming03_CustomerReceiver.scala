package com.atguigu.sparkstreaming

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author layne
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    //TODO 2 利用SparkConf创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

    val resultDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)


    resultDStream.print()



    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()
  }

}

class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //启动receiver以后,立即执行的方法
  override def onStart(): Unit = {
    //在onStart方法里面创建一个线程,专门用来接收数据
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()

  }

  def receive(): Unit = {
    //实现一个对接外部端口号的receiver
    val socket = new Socket(host,port)
    //获取socket里面的数据
    val inputStream: InputStream = socket.getInputStream
    //按行读取inputStream字节输入流的数据
    val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    var input: String = reader.readLine()

    //循环读取字节输入流的数据,直到没有
    //当receiver没有关闭并且输入数据不为空，就循环发送数据给Spark
    while(!isStopped() && input != null){
      //将读进来的一行数据存到spark中
      store(input)
      input = reader.readLine()

    }

    // 关闭资源
    reader.close()
    socket.close()

    //实现recever的重启方法
    restart("重启Receiver")

  }


  override def onStop(): Unit = {

  }
}
