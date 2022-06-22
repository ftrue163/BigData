package chapter04

import scala.collection.immutable

/**
 * @author yhm
 * @create 2021-09-13 11:04
 */
object Test02_ForLoop {
  def main(args: Array[String]): Unit = {

    // for循环的基本语法
    val inclusive: Range.Inclusive = 0 to 5

    for (i <- 0.to(5)) {
      println(i)
    }

    for (i <- 0 until 5) {
      println(i)
    }

    // for循环的本质
    // 遍历一个集合   增强for循环
    for (i <- Array(1, 2, 3, 4)) {
      println(i)
    }

    println("========================")

    // 循环守卫
    for (i <- 0 to 10) {
      if (i % 2 == 0) {
        println(i)
      }
    }


    for (i <- 0 to 10 if i % 2 == 0) {
      println(i)
    }

    // 循环步长
    for (i <- 0 to 10 by 2) {
      println(i)
    }

    for (i <- 0.5 to 9.5 by 0.1) {
      println(i)
    }

    for (i <- 10 to 0 by -1) {
      println(i)
    }


    // 嵌套循环
    for (i <- 0 to 3) {
      for (j <- 0 to 4) {
        print(s"$i - $j \t")
      }
      println()
    }

    for (i <- 0 to 3; j <- 0 to 4) {
      print(s"$i - $j \t")

      if (j == 4) {
        println()
      }
    }


    // 引入变量
    for (i <- 0 to 5) {
      val j = 3 * i + 5
      println(s"$i - $j")
    }


    for (i <- 0 to 5; j = 3 * i + 5) {
      println(s"$i - $j")
    }


    for {
      i <- 0 to 5
      j = 3 * i + 5
    } {
      println(s"$i - $j")
    }


    // 循环返回值  想要返回值  必须要加yield关键字
    val ints: immutable.IndexedSeq[Int] = for (i <- 0 to 5) yield {
      i
    }

    println(ints)


    // 倒序打印
    for (i <- 0 to 5 reverse){
      println(i)
    }


  }

}
