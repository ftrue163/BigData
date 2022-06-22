package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author layne
 */
object SparkSQL13_TopN {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    //注册udaf
    spark.udf.register("city_remark",functions.udaf(new CityRemarkUDAF))

    //连接外部hive
    spark.sql(
      """
        |select
        |    t3.area,
        |    t3.product_name,
        |    t3.click_count,
        |    t3.city_remark,
        |    t3.rk
        |from
        |(
        |    select
        |        t2.area,
        |        t2.product_name,
        |        t2.click_count,
        |        t2.city_remark,
        |        rank() over(partition by t2.area order by t2.click_count desc) rk
        |    from
        |    (
        |        select
        |            t1.area,
        |            t1.product_name,
        |            count(*) click_count,
        |            city_remark(t1.city_name) city_remark
        |        from
        |        (
        |            select
        |               c.area,
        |               c.city_name,
        |               p.product_name,
        |               v.click_product_id
        |            from
        |            (select * from user_visit_action where click_product_id>-1) v
        |            join city_info c
        |            on v.city_id=c.city_id
        |            join product_info p
        |            on v.click_product_id=p.product_id
        |        )t1
        |        group by t1.area,t1.product_name
        |    )t2
        |)t3
        |where t3.rk<=3
        |""".stripMargin).show(1000,false)




    //TODO 3 关闭资源
    spark.stop()
  }

}

/**
 * 自定义udaf
 * 输入: String
 * 缓存区:Buffer
 * 输出: String
 */
case class Buffer(var totalCnt:Long,var cityMap:mutable.Map[String,Long])

class CityRemarkUDAF extends Aggregator[String,Buffer,String]{
  override def zero: Buffer = Buffer(0L,mutable.Map[String,Long]())

  override def reduce(buffer: Buffer, city_name: String): Buffer = {
    buffer.totalCnt += 1
    buffer.cityMap(city_name) = buffer.cityMap.getOrElse(city_name,0L) + 1
    buffer
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    b1.totalCnt += b2.totalCnt
    b2.cityMap.foreach{
      case (cityname,citycnt)=>{
        b1.cityMap(cityname)=b1.cityMap.getOrElse(cityname,0L) + citycnt
      }
    }
    b1
  }

  override def finish(buffer: Buffer): String = {

    //0 定义一个Stirng类型的ListBuffer,最终用来返回
    val cityRemark: ListBuffer[String] = ListBuffer[String]()
    //1 对城市map按照城市的点击次数倒序排序
    val sortList: List[(String, Long)] = buffer.cityMap.toList.sortBy(_._2)(Ordering[Long].reverse)

    //2 取出list的前两名的城市,求出城市占比
    var sum =0L
    sortList.take(2).foreach{
      case (cityname,citycnt)=>{
        val res: Long = citycnt*100/buffer.totalCnt
        cityRemark.append(cityname + " "+ res+"%")

        sum += res
      }
    }

    //3 求其他城市占比
    if(sortList.size>2){

      cityRemark.append("其他 "+(100-sum)+"%")

    }

    //4 返回cityRemark
    cityRemark.mkString(",")


  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}
