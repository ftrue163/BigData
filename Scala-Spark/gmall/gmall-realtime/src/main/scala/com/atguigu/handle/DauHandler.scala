package com.atguigu.handle

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

object DauHandler {
    /**
     * 进行批次内去重
     * @return
     */
    def filterbyGroup(startUpLogDStream: DStream[StartUpLog]) = {
        //1.将数据转化为k，v ((mid,logDate)log)
        val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = startUpLogDStream.mapPartitions(partition => {
            partition.map(log => ((log.mid, log.logDate), log))
        })

        //2.groupByKey将相同key的数据聚和到同一个分区中
        //3.将数据排序并取第一条数据
        val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogDStream.groupByKey().mapValues(iter => {
            iter.toList.sortWith(_.ts < _.ts).take(1)
        })

        //4.将数据扁平化
        val value: DStream[StartUpLog] = midAndDateToLogListDStream.flatMap(_._2)

        value
    }

    /**
     * 进行批次间去重
     *
     * @param startUpLogDStream
     */
    def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
        //方案1：每条数据 创建一次Jedis连接
        /*val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
            //创建redis连接
            val jedisClient = new Jedis("hadoop102", 6379, 1000 * 10)

            //获取redisKey
            val redisKey = "DAU:" + log.logDate

            //对比数据，重复的去掉，不重复的留下来
            val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)

            //关闭redis连接
            jedisClient.close()

            !boolean
        })
        value*/

        //方案2(优化)：每个批次的每个分区 创建一次Jedis连接
        /*val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
            //创建redis连接
            val jedisClient = new Jedis("hadoop102", 6379, 1000 * 10)

            //对比数据，重复的去掉，不重复的留下来
            val logs: Iterator[StartUpLog] = partition.filter(log => {
                //获取redisKey
                val redisKey = "DAU:" + log.logDate
                //对比数据，重复的去掉，不重复的留下来
                val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
                !boolean
            })

            //关闭redis连接
            jedisClient.close()
            logs
        })
        value*/


        //方案3(优化)：每个批次 创建一次Jedis连接
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
            //创建redis连接
            val jedisClient = new Jedis("hadoop102", 6379, 1000 * 10)

            //获取redisKey
            val redisKey: String = "DAU:" + sdf.format(new Date())

            //查redis中的mid
            val mids: util.Set[String] = jedisClient.smembers(redisKey)

            //将数据广播至executor端
            val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

            //根据获取到的mid去重
            val midFilterRDD: RDD[StartUpLog] = rdd.mapPartitions(partition => {
                partition.filter(log => {
                    !midBC.value.contains(log.mid)
                })
            })

            //关闭redis连接
            jedisClient.close()
            midFilterRDD
        })
        value

    }

    /**
     * 将去重后的数据保存至redis，为下一批数据去重用
     *
     * @param startUpLogDStream
     */
    def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
        startUpLogDStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                //1.创建连接
                val jedisClient: Jedis = new Jedis("hadoop102", 6379, 1000 * 10)
                //2.写库
                partition.foreach(startUpLog => {
                    //redisKey
                    val redisKey = "DAU:" + startUpLog.logDate
                    //将mid存入redis
                    jedisClient.sadd(redisKey, startUpLog.mid)
                })
                //3.关闭连接
                jedisClient.close()
            })
        })
    }

}
