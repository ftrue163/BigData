package chapter05

/**
 * @author yhm
 * @create 2021-09-13 15:22
 */
object Test03_FunArgs {
  def main(args: Array[String]): Unit = {
    // 可变参数
    def sayHi(names:String*):Unit = {
      println(s"hi $names")
      // 可变参数在函数值本质是一个数组
      for (elem <- names) {

      }
    }

    sayHi()
    sayHi("linhai")
    sayHi("linhai","jinlian")

    // 可变参数使用:
    // 可变参数必须在参数列表的最后
    def sayHi1(sex: String,names:String*):Unit = {
      println(s"hi $names")
    }

    // 参数默认值
    def sayHi2(name:String = "linhai"):Unit = {
      println(s"hi ${name}")
    }

    sayHi2("linhai")
    sayHi2()


    // 可变参数在使用的时候 可以不在最后
    def sayHi3( name:String = "linhai" , age:Int):Unit = {
      println(s"hi ${name}")
    }
    // 带名参数
    sayHi3(age = 10)


  }
}
