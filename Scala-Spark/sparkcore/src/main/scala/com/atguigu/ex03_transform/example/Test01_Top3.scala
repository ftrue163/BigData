package com.atguigu.ex03_transform.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 2.4.4 案例实操（广告点击Top3）
 * 需求: 求不同的省份广告点击的top3
 */
object Test01_Top3 {
    /*def main(args: Array[String]): Unit = {
        // 1. 创建spark配置对象
        val conf: SparkConf = new SparkConf().setAppName("SparkCore").setMaster("local[*]")

        // 2. 创建sparkContext
        val sc = new SparkContext(conf)

        val lineRDD: RDD[String] = sc.textFile("sparkcore/input/agent.txt")


        // 步骤一: 选取有用的数据
        val tupleRDD: RDD[(String, String)] = lineRDD.map(s => {
            val strings: Array[String] = s.split(" ")
            (strings(1), strings(4))
        })

        // 步骤二: 聚合统计广告出现多少次
        // 原数据格式 (省份,广告id)
        // 转换格式 ("省份_广告id",1)
        // 转换格式 ((省份,广告id),1)
        val countRDD: RDD[((String, String), Int)] = tupleRDD.map(tuple => (tuple, 1))
            .reduceByKey(_ + _)

        // 合并前两步
        val countRDD1: RDD[((String, String), Int)] = lineRDD.map(s => {
            val strings: Array[String] = s.split(" ")
            ((strings(1), strings(4)), 1)
        }).reduceByKey(_ + _)

        //    countRDD.collect().foreach(println)

        // 步骤三: 聚合相同的省份
        // 转换格式 使用groupByKey 相对简单一些
        // 原(省份,广告id),1)
        // 转换格式  (省份,(广告id,次数))
        val idCountRDD: RDD[(String, (String, Int))] = countRDD.map({
            case (tuple, count) => (tuple._1, (tuple._2, count))
        })

        // 聚合
        val provinceRDD: RDD[(String, Iterable[(String, Int)])] = idCountRDD.groupByKey()

        // provinceRDD.collect().foreach(println)


        //步骤四: 针对已经分好组的数据  排序取top3
        // list格式  (id,count)
        val result: RDD[(String, List[(String, Int)])] = provinceRDD.mapValues(list => {
            // 集合常用函数
            list.toList
                .sortBy(_._2)(Ordering[Int].reverse)
                .take(3)
        })

        result.collect().foreach(println)

        Thread.sleep(600000)

        // 4. 关闭sc
        sc.stop()
    }*/

    def main(args: Array[String]): Unit = {

        //1. 初始化Spark配置信息并建立与Spark的连接
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
        val sc = new SparkContext(sparkConf)

        //2. 读取日志文件，获取原始数据
        val dataRDD: RDD[String] = sc.textFile("sparkcore/input/agent.txt")

        //3. 将原始数据进行结构转换string =>(prv-adv,1)
        val prvAndAdvToOneRDD: RDD[(String, Int)] = dataRDD.map {
            line => {
                val datas: Array[String] = line.split(" ")
                (datas(1) + "-" + datas(4), 1)
            }
        }

        //4. 将转换结构后的数据进行聚合统计（prv-adv,1）=>(prv-adv,sum)
        val prvAndAdvToSumRDD: RDD[(String, Int)] = prvAndAdvToOneRDD.reduceByKey(_ + _)

        //5. 将统计的结果进行结构的转换（prv-adv,sum）=>(prv,(adv,sum))
        val prvToAdvAndSumRDD: RDD[(String, (String, Int))] = prvAndAdvToSumRDD.map {
            case (prvAndAdv, sum) => {
                val ks: Array[String] = prvAndAdv.split("-")
                (ks(0), (ks(1), sum))
            }
        }

        //6. 根据省份对数据进行分组：(prv,(adv,sum)) => (prv, Iterator[(adv,sum)])
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvToAdvAndSumRDD.groupByKey()

        //7. 对相同省份中的广告进行排序（降序），取前三名
        val mapValuesRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues {
            datas => {
                datas.toList.sortWith(
                    (left, right) => {
                        left._2 > right._2
                    }
                ).take(3)
            }
        }

        //8. 将结果打印
        mapValuesRDD.collect().foreach(println)

        //9.关闭与spark的连接
        sc.stop()
    }
}
