package chapter05

/**
 * @author yhm
 * @create 2021-09-15 11:09
 */
object Test07_Closer {
  def main(args: Array[String]): Unit = {

    // 定义一个嵌套的函数
    def f1(i: Int): Char => (String => Boolean) = {

      val n = i
      def f2(c: Char): String => Boolean = {
        def f3(s: String): Boolean = {
          n != 0 || c != '0' || s != ""
        }

        f3 _
      }

      f2 _
    }

    val f2: Char => String => Boolean = f1(5)


    // 两个数相加  泛用性更强
    def sumAB(a:Int,b:Int) :Int = a + b

    // 确定一个数是4  适用性更强
    def sumByFour(b:Int) :Int = 4 + b
    def sumByFive(b:Int) :Int = 5 + b

    // 定义一个函数 动态确定一个参数
    def sumByA(a:Int): Int => Int = {
      def  sumAB(b:Int):Int = a + b
      sumAB _
    }


    val sumByFour1: Int => Int = sumByA(4)
    val sumByFive1: Int => Int = sumByA(5)


  }
}
