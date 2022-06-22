package chapter09add

import sun.misc.IOUtils

/**
 * @author yhm
 * @create 2021-09-18 16:10
 */
object Test02_Imp {
  def main(args: Array[String]): Unit = {
    // 隐式函数
    implicit def changeInt(i:Int):MyRichInt = {
      new MyRichInt(i)
    }

    val i:Int = 10
    // 隐式转换的目的  在不修改原类代码的基础上  给这个类添加更多的方法

    // 和填入的参数比较大小  返回较大的值
    val i1: Int = i.myMax(20)
    println(i1)

    i.max(30)

    // 隐式参数
    implicit val name1:String = "linhai"

    def sayHi(implicit name:String = "zhangsan")={
      println(s"hi $name")
    }

    sayHi
    sayHi()

    // 隐式类
    val d: Double = 3.14
    d.sayHi

  }

  class MyRichInt(val self:Int) {

    def myMax(i:Int):Int = {
      if (self > i) self else i
    }
  }

  implicit class MyRichDouble(self:Double){
    def sayHi: Unit ={
      println("hello double")
    }
  }


}


