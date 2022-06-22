package chapter04

import scala.io.StdIn

/**
 * @author yhm
 * @create 2021-09-13 10:24
 */
object Test01_If {
  def main(args: Array[String]): Unit = {
    //    需求：输入人的年龄，如果该同志的年龄小于18岁，则输出“童年”

    println("请输入您的年龄: ")

    val age: Int = StdIn.readInt()

    if (age < 18) {
      println("童年")
    } else {
      println("成年")
    }


    //    需求：输入年龄，如果年龄小于18岁，则输出“童年”。
    //    如果年龄大于等于18且小于等于60，则输出“中年”，否则，输出“老年”。
    if (age < 18) {
      println("童年")
    } else if (age < 60) {
      println("中年")
    } else {
      println("老年")
    }


    // if else 的返回值取决于 每一个条件的最后一行代码
    // 如果多个判断条件的返回类型不一致  需要使用他们共同的父类
    val str: Any = if (age < 18) {
      "童年"
    } else if (age < 60) {
      "中年"
    } else if (age < 120) {
      "老年"
    } else {
      666
    }

    println(str)


    // nothing是所以类的共同子类  所以不影响返回类型
    val str1: String = if (age >= 0) {
      "人"
    } else {
      throw new RuntimeException()
    }

    //    （4）需求4：Java中的三元运算符可以用if else实现
    //      如果大括号{}内的逻辑代码只有一行，大括号可以省略。如果省略大括号，if只对最近的一行逻辑代码起作用。
    //    (age < 18) ? "童年":"成年"
    if (age < 18) "童年" else "成年"


    // 嵌套分支
    if (age < 18) {
      "童年"
    } else {
      if (age < 60){
        "中年"
      }else {
        "老年"
      }
    }

  }
}
