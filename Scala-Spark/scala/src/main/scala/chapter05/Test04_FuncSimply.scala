package chapter05

/**
 * @author yhm
 * @create 2021-09-13 15:57
 */
object Test04_FuncSimply {
  def main(args: Array[String]): Unit = {
    // 定义一个原函数
    def func0(x: Int, y: Int): Int = {
      println("func0的调用")
      if (x < 20) {
        return x + y
      }

        2 * x + 3 * y

    }

    println(func0(10, 20))

    //    （1）return可以省略，Scala会使用函数体的最后一行代码作为返回值
    def func1(x: Int, y: Int): Int = {
      println("func1的调用")
      x + y
    }

    val i: Int = func1(10, 20)
    println(i)


    //    （2）如果函数体只有一行代码，可以省略花括号
    // 如果不写大括号 默认有效范围只有一行
    def func2(x: Int, y: Int): Int = x + y


    //    （3）返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    def func3(x: Int, y: Int) = x + y


    //    （4）如果有return，则不能省略返回值类型，必须指定
    def func4(x: Int, y: Int): Int = {
      if (x < 20) {
        return x + y
      }
      2 * x + 3 * y
    }

    func4(10, 20)

    //    （5）如果函数明确声明unit，那么即使函数体中使用return关键字也不起作用
    def func5(x: Int, y: Int): Unit = return x + y

    val unit: Unit = func5(10, 20)

    //    （6）Scala如果期望是无返回值类型，可以省略等号
    def func6(x:Int,y:Int){
      println(x+y)
    }


    //    （7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    def func7():Unit = {
      println("hello")
    }

    func7()
    func7

    //    （8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    def func8{
      println("hello")
    }

    func8

    //    （9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
    def func9(x: Int, y: Int) = x + y


    val function: (Int, Int) => Int = (x: Int, y: Int) => x + y

  }
}
