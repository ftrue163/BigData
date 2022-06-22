package chapter04

/**
 * @author yhm
 * @create 2021-09-13 14:21
 */
object Test03_While {
  def main(args: Array[String]): Unit = {
    // while的基本语法
    var i = 0

    while (i < 5) {
      println(i)
      i += 1
    }

    i = 0
    while (i < 5) {
      println(i)
      i += 1
    }

    println("*************")
    // do while 不管是否满足判断条件 都会执行一次循环体
    do {
      println(i)
    }while(i < 5)

  }
}
