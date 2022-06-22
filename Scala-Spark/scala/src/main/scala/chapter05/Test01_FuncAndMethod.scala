package chapter05

/**
 * @author yhm
 * @create 2021-09-13 15:04
 */
object Test01_FuncAndMethod {
  def main(args: Array[String]): Unit = {

    // 定义了一个函数
    // 函数不允许重载
    def sayHi(name: String): Unit = {
      println(s"hi ${name}")
    }

//    def sayHi(name: String, age: Int): Unit = {
//      println(s"hi  $age 岁的 $name")
//    }

    sayHi("linhai")

    sayHi1("linhai", 28)

    // 在函数当中也能够创建函数
    def sayHi2(name: String): Unit = {
      def changeName(): String ={
        s"$name 大帅哥"
      }

      println(s"hi ${changeName()}")
    }

    sayHi2("linhai")



  }

  // 定义在类当中的叫做方法
    def sayHi1(name:String):Unit = {
      println(s"hi ${name}")
    }

    def sayHi1(name:String, age:Int) :Unit ={

    // 在方法当中也可以定义函数
      def changeName(): String ={
        s"$name 大帅哥"
      }
      println(s"hi  $age 岁的 ${changeName()}")
    }


}
