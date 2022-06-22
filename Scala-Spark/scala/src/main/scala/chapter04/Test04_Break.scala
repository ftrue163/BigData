package chapter04

import scala.util.control.Breaks

/**
 * @author yhm
 * @create 2021-09-13 14:26
 */
object Test04_Break {
  def main(args: Array[String]): Unit = {
    // 需求: 当i = 5的时候 跳出整个循环
//    for (i <- 0 to 10) {
//      if (i < 5){
//        println(i)
//      }
//
//      println("循环一次")
//    }


    try {
      for (i <- 0 to 10){
        if (i == 5){
          throw new RuntimeException
        }
        println(i)
        println("循环一次")
      }
    }catch {
      case e:RuntimeException => println("捕获异常")
    }

    println("其他代码")


    // 使用scala对象的方法完成循环中断

    Breaks.breakable({
      for (i <- 0 to 10){
        if (i == 5) Breaks.break()
        println(i)
      }
    })

    import Breaks._

    breakable({
      for (i <- 0 to 10){
        if (i == 5) break()
        println(i)
      }
    })

  }
}
