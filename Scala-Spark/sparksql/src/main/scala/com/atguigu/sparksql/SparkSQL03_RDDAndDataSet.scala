package com.atguigu.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author layne
 */
object SparkSQL03_RDDAndDataSet {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //todo rdd=>DS
    //创建RDD
    val sc: SparkContext = spark.sparkContext
    val lineRDD: RDD[String] = sc.textFile("D:\\IdeaProjects\\SparkSQL0625\\input\\user.txt")
    //转化rdd数据结构
    val rdd: RDD[(String, String)] = lineRDD.map(
      line => {
        val datas: Array[String] = line.split(",")
        (datas(0), datas(1))
      }
    )
    // 转换数据结构得到userRDD
    val userRDD: RDD[User] = rdd.map {
      case (name, age) => User(name, age.toLong)
    }

    //注意:在做转换之前,一定要导入spark对象的全部隐式转换
    import spark.implicits._


    val ds: Dataset[(String, String)] = rdd.toDS()
    ds.show()

    val userDS: Dataset[User] = userRDD.toDS()

    userDS.show()

    //todo DS=>RDD
    val rdd2: RDD[(String, String)] = ds.rdd

    val userRDD2: RDD[User] = userDS.rdd
    userRDD2.collect().foreach(println)


    //TODO 3 关闭资源
    spark.stop()
  }

}
