package chapter02

import scala.io.StdIn

/**
 * @author yhm
 * @create 2021-09-11 14:15
 */
object Test04_StdIn {
  def main(args: Array[String]): Unit = {
    // 交互系统  输入姓名和年龄
    println("欢迎来到尚硅谷!")

    println("请输入您的姓名:")

    val name: String = StdIn.readLine()

    println("请输入您的年龄:")

    val age: Int = StdIn.readInt()

    println(s"欢迎${age}岁的${name}同学来尚硅谷玩")


  }
}
