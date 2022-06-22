package chapter05

/**
 * @author yhm
 * @create 2021-09-13 16:15
 */
object Test05_FuncSimply2 {
  def main(args: Array[String]): Unit = {
    val f0: (Int, Int) => Int = (x: Int, y: Int) => x + y

    //    （1）参数的类型可以省略，会根据形参进行自动的推导
    val f1: (Int, Int) => Int = (x, y) => x + y

    //    （2）类型省略之后，发现只有一个参数，则圆括号可以省略；
    //    其他情况：没有参数和参数超过1的永远不能省略圆括号。
    val f2: (Int, Int) => Int = (x, y) => x + y
    val f3: Int => Int = x => x + 22

    val f4: () => Int = () => 10


    //    （3）匿名函数如果只有一行，则大括号也可以省略
    val f5: (Int, Int) => Int = (x, y) => {
      println("匿名函数")
      x + y
    }

    //    （4）如果参数只出现一次，则参数省略且后面参数可以用_代替
    val f6: (Int, Int) => Int = _ + _


    // 化简为_的条件
    // 1. 传入的参数类型可以推断 所以可以省略
    val f7: (Int, Int) => Int = (x, y) => y - x

    // 2. 参数必须只使用一次  使用的顺序必要和定义的顺序一样
    val f8: (Int, Int) => Int = -_ + _


    // 如果化简为匿名函数  只剩下一个_  则不可以化简
    val function: String => String = _ + ""
    val str: String = function("linhai")
    val function1: String => String = a => a


    // 如果化简的下划线在函数里面  也会报错
//    val function1: String => Unit = println(_ + "hi")

    val function2: String => Unit = println
    function2("linhai")


  }
}
