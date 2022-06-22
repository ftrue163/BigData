package com.sit

import org.apache.kafka.clients.consumer.ConsumerConfig

import java.text.SimpleDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.util.{Calendar, Properties}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}

/**
 * Created by Jiangfy on 2021/3/25 14:54
 */
object HZDataObjectLgBkry {
    //private var kafkaStream: InputDStream[(String, String)] = null
    //private var offsetRanges: Array[OffsetRange] = null
    ////指定kafka的broker地址(SparkStreaming的Task直接连接到kafka的分区上，用更加底层的API消费，效率更高)
    //val brokerList = "41.228.2.98:9092,41.228.2.99:9092,41.228.2.51:9092"
    ////指定zookeeper的地址，用来存放数据偏移量数据，也可以使用Redis MySQL等
    ////val zkQuorum = "41.228.2.97:2181,41.228.2.51:2181,41.228.2.52:2181"
    //val consumerTopic: Set[String] = Set("ysk_gj_lgnbzsxxb_ds", "qx_lg_test")
    //val producterTopic: String = "ywk_bkyj_shlgyj_ds"

    def main(args: Array[String]): Unit = {
        /*Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
        Logger.getLogger("org.apache.kafka.clients.consumer").setLevel(Level.ERROR)
        //System.setProperty("hadoop.home.dir", "D:\\软件\\hadoop\\hadoop-common-2.2.0-bin-master")
        //创建SparkConf 提交到集群中运行
        //val conf = new SparkConf().setAppName("KafakaDirect8").setMaster("local[*]")
        val conf = new SparkConf().setAppName("HZDataObjectLgBkry").set("spark.port.maxRetries", "128")
        val sc = new SparkContext(conf)
        //设置SparkStreaming，并设定间隔时间
        val ssc = new StreamingContext(sc, Seconds(2))
        //准备kafka的参数
        val kafkaParams = Map[String, String](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
            ConsumerConfig.GROUP_ID_CONFIG -> "HzLgData",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
            "auto.offset.reset" -> "largest",
            "enable.auto.commit" -> "true"
            //"security.protocol" -> "",
            //"sasl.mechanism" -> "",
            //"sasl.tbds.secure.id" -> Init.kafkaSecureId,
            //"sasl.tbds.secure.key" -> Init.kafkaSecurekey
        )
        //创建sqlContext对象
        //val sqlContext = new SQLContext(sc)
        //dval prop = new java.util.Properties
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, consumerTopic)
        //直接操作kafkaStream
        //依次迭代DStream中的kafkaRDD 只有kafkaRDD才可以强转为HasOffsetRanges  从中获取数据偏移量信息
        //之后是操作的RDD 不能够直接操作DStream 因为调用Transformation方法之后就不是kafkaRDD了获取不了偏移量信息
        kafkaStream.foreachRDD(kafkaRDD => {
            try {
                if (!kafkaRDD.isEmpty()) {
                    val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
                    val mapData: RDD[String] = kafkaRDD.map(_._2)
                    mapData.foreachPartition(data => {
                        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
                        val conn = JedisConnectionPool.getConnections()
                        data.foreach(record => {
                            val recordJson: JSONObject = JSON.parseObject(record)
                            //println(recordJson.getString(("id")))
                            val idNumber: String = recordJson.getString("gmsfhm")
                            //println("到这里了!")
                            if (idNumber != null && idNumber.size > 0) {
                                val allValueZdry: String = conn.get(idNumber)
                                if (allValueZdry != null && allValueZdry.size > 0) {
                                    val kafkaValue: JSONObject = new JSONObject()
                                    //val content2: JSONObject = new JSONObject()
                                    //拼接预警信息
                                    val tsbh: String = recordJson.getString("zklsh")
                                    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                    //val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                    val TIME_KEY_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
                                    val cal: Calendar = Calendar.getInstance()
                                    val tssj = dateFormat.format(cal.getTime())
                                    val deadline: String = ""
                                    val title: String = "高危涉黄人员旅馆记录预警"
                                    val dst: String = recordJson.getString("ssxq_pcsdm")
                                    val src: String = "模型工厂"
                                    val types: String = "涉黄"
                                    val content: String = idNumber
                                    val fk: String = ""
                                    //val xq: String = ""

                                    //拼接content2
                                    val xm: String = recordJson.getString("xm")
                                    val mz: String = recordJson.getString("mz")
                                    val xb: String = recordJson.getString("xb")
                                    val jg_xzqhmc: String = recordJson.getString("jg_xzqhmc")
                                    val zslgmc: String = recordJson.getString("zslgmc")
                                    val zslgbm: String = recordJson.getString("zslgbm")
                                    val zslgdz: String = recordJson.getString("zslgdz")
                                    val fjh: String = recordJson.getString("fjh")
                                    val ssxq_pcsmc: String = recordJson.getString("ssxq_pcsmc")
                                    var rzsj: String = recordJson.getString("rzsj")
                                    if (rzsj.length != 0) {
                                        rzsj = dateFormat.format(TIME_KEY_FORMAT.parse(rzsj))
                                    }
                                    val ldsj: String = recordJson.getString("ldsj")
                                    val xq: String = jg_xzqhmc + "-" + xm + "-" + zslgmc + "-" + fjh + "-" + rzsj
                                    /*                content2.put("xm",xm)
                                                      content2.put("mz",mz)
                                                      content2.put("xb",xb)
                                                      content2.put("jg_xzqhmc",jg_xzqhmc)
                                                      content2.put("zslgmc",zslgmc)
                                                      content2.put("zslgbm",zslgbm)
                                                      content2.put("zslgdz",zslgdz)
                                                      content2.put("fjh",fjh)
                                                      content2.put("ssxq_pcsmc",ssxq_pcsmc)
                                                      content2.put("rzsj",rzsj)
                                                      content2.put("ldsj",ldsj)*/

                                    //val content2_value: String = JSON.toJSONString(content2, SerializerFeature.WriteNullStringAsEmpty)
                                    val content2: String = ""
                                    kafkaValue.put("tsbh", tsbh)
                                    kafkaValue.put("tssj", tssj)
                                    kafkaValue.put("deadline", deadline)
                                    kafkaValue.put("title", title)
                                    kafkaValue.put("dst", dst)
                                    kafkaValue.put("src", src)
                                    kafkaValue.put("type", types)
                                    kafkaValue.put("content", content)
                                    kafkaValue.put("fk", fk)
                                    kafkaValue.put("xq", xq)
                                    kafkaValue.put("content2", content2)

                                    val value = JSON.toJSONString(kafkaValue, SerializerFeature.WriteNullStringAsEmpty)
                                    //println(value)
                                    //println(s"topic: ${o.topic} partition: ${o.partition} fromOffset: ${o.fromOffset} untilOffset: ${o.untilOffset}")
                                    //println("来了")
                                    KafkaTools.send(producterTopic, "idNumber", value)
                                }
                            }
                        })
                        JedisConnectionPool.returnConnection(conn)
                    })
                }
            }
            catch {
                case ex: Exception => println(ex)
            }
        })
        ssc.start()
        ssc.awaitTermination()*/
    }

    /*//Jedis连接池
    object JedisConnectionPool {
        //连接配置
        val config = new JedisPoolConfig
        //最大连接数
        config.setMaxTotal(20)
        //最大空闲连接数
        config.setMaxIdle(10)
        //设置连接池属性分别有： 配置  主机名   端口号  连接超时时间    Redis密码
        val pool = new JedisPool(config, "41.228.2.91", 6379, 10000, "Sailing123`")

        //连接池
        def getConnections(): Jedis = {
            pool.getResource
        }

        def returnConnection(conn: Jedis): Unit = {
            pool.returnResource(conn)
        }
    }

    /*
    * Created by Jiangfy on 2021/3/23 11:42
    * Kafka工具类
      */
    object KafkaTools extends Serializable {
        val props: Properties = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
        //props.put(ProducerConfig.CLIENT_ID_CONFIG, "0")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        //lazy val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
        lazy val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props) //构造生产者

        def close(): Unit = {
            if (producer != null) {
                producer.close()
            }
        }

        def send(topicName: String, key: String, value: String) = {
            producer.send(new ProducerRecord[String, String](topicName, key, value))
        }
    }*/
}