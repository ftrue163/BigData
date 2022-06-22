package chapter05

/**
 * @author yhm
 * @create 2021-09-15 14:16
 */
object Test09_ControlAbs {
  def main(args: Array[String]): Unit = {
    // 值调用
    def sayHi(name:String):Unit = {

      println("sayHi的调用")
      println(s"hi $name")
      println(s"hi $name")
    }

    sayHi({
      println("代码块-字符串")
      "linhai"
    })


    println("=======================")
    // 名调用    => String
    def sayHi1(name: => String):Unit = {
      println("sayHi1的调用")
      println(s"hi $name")
      println(s"hi $name")
    }

    var n = 1
    sayHi1({
      println("代码块-字符串1")
      n += 1
      "linhai" + n
    })

  }

}
