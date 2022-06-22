package chapter08

/**
 * @author yhm
 * @create 2021-09-18 14:21
 */
object Test01_Match {
  def main(args: Array[String]): Unit = {
    val x = 10
    val y = 20
    val c = '.'

    // 模式匹配也有返回值
    val value: Any = c match {
      case '+' => x + y
      case '-' => x - y
      case '*' => x * y
      case '/' => x / y
      case _ => "请输入正确的符号"
    }

    println(value)


    // 传入一个整数  返回它的绝对值
    def func1(n:Int):Int = {
      n match {
        case i:Int if i >= 0 => i
        case i:Int if i < 0 => -i
      }
    }

    println(func1(-100))
    println(func1(100))
    println(func1(0))


  }
}
