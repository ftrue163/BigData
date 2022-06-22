package chapter09add

/**
 * @author yhm
 * @create 2021-09-18 16:02
 */
object Test01_Exception {
  def main(args: Array[String]): Unit = {
    // 基本语法
    try{
       1 / 0
//     val a:Byte = 128
    }catch{
          // 模式匹配 捕获异常
      case e: ArithmeticException => println("捕获异常")
      case _ =>
    }finally {
      // 无论发生什么情况 都会执行
      println("结束")
    }

    func1()
  }

  @throws // 标记当前方法会抛出异常
  def func1():Unit={
    "abc".toInt
  }

}
