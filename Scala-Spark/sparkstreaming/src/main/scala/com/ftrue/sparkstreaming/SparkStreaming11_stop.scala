package com.atguigu.sparkstreaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @author layne
 */
object SparkStreaming11_stop {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")

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

    //5 打印当前数据流
    resultDStream.print()

    // 开启监控程序
    new Thread(new MonitorStop(ssc)).start()


    //TODO 3 启动StreamingContext,并且阻塞主线程,一直执行
    ssc.start()
    ssc.awaitTermination()

  }

}

class MonitorStop(ssc: StreamingContext) extends Runnable{
  override def run(): Unit = {

    // 获取HDFS文件系统
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"),new Configuration(),"atguigu")

    while(true){
      Thread.sleep(5000)

      // 获取/stopSpark路径是否存在
      val result: Boolean = fs.exists(new Path("hdfs://hadoop102:8020/stop0625"))

      if (result){

        //获取当前ssc的状态
        val state: StreamingContextState = ssc.getState()
        //如果当前ssc的状态等于激活状态,优雅的关闭当前ssc的程序
        if(state == StreamingContextState.ACTIVE){
          ssc.stop(true,true)
          System.exit(0)

        }
      }

    }
  }
}
