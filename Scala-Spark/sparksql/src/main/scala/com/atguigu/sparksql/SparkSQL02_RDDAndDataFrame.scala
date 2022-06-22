package com.atguigu.sparksql


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author layne
 */
object SparkSQL02_RDDAndDataFrame {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //todo rdd=>DF
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

    userRDD.collect().foreach(println)

    //注意:在做转换之前,一定要导入spark对象的全部隐式转换
    import spark.implicits._

    val df: DataFrame = rdd.toDF("name","age")

    df.show()

    val userDF: DataFrame = userRDD.toDF()
    userDF.show()

    //todo DF=>RDD
    val rdd2: RDD[Row] = df.rdd
    val userRDD2: RDD[Row] = userDF.rdd

    userRDD2.collect().foreach(println)

    //将userRDD2再次转换userRDD
    userRDD2.map(
      row =>{
        User(row.getString(0),row.getLong(1))
      }
    ).collect().foreach(println)



    //TODO 3 关闭资源
    spark.stop()
  }

}

case class User(name:String,age:Long)
