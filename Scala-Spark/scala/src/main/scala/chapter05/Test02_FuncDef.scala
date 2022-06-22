package chapter05

/**
 * @author yhm
 * @create 2021-09-13 15:14
 */
object Test01_FuncDef {
  def main(args: Array[String]): Unit = {
    //    （1）函数1：无参，无返回值
    def sayHi(): Unit = {
      println("hello world")
    }

    sayHi()
    sayHi

    //    （2）函数2：无参，有返回值
    def sayHi1(): String = {
      "hi"
    }

    val str: String = sayHi1()
    val str1: String = sayHi1

    println(str)
    println(str1)

    //    （3）函数3：有参，无返回值
    def sayHi2(name: String): Unit = {
      println(s"hi ${name}")
    }

    val linhai: Unit = sayHi2("linhai")

    //    （4）函数4：有参，有返回值
    def sayHi3(name: String): String = {
      s"hi ${name}"
    }

    val linhai1: String = sayHi3("linhai")

    //    （5）函数5：多参，无返回值
    def sayHi4(name: String, age: Int): Unit = {
      println(s"hi $age 的$name")
    }

    //    （6）函数6：多参，有返回值
    def sayHi5(name:String,age:Int) : String = {
      s"hi $age 的 $name"
    }

  }
}
