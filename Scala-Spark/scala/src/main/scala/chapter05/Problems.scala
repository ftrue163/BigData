package chapter05

/**
 * @author yhm
 * @create 2021-09-15 10:02
 */
object Problems {
  def main(args: Array[String]): Unit = {
    // 需求一 : 定义一个函数  传入的参数是(0,'0',"") 返回false  否则返回true
    def func1(i: Int, c: Char, s: String): Boolean = {
      if (i == 0 && c == '0' && s == "") {
        return false
      } else return true
    }

    println(func1(0, '0', ""))
    println(func1(1, '0', ""))
    println(func1(0, '1', ""))
    println(func1(0, '0', "1"))

    def func2(i: Int, c: Char, s: String): Boolean =
      if (i == 0 && c == '0' && s == "") false else true

    def func3(i: Int, c: Char, s: String): Boolean =
    //      !(i == 0 && c == '0' && s == "" )
      i != 0 || c != '0' || s != ""

    // 过度化简  可读性太差
    val function: (Int, Char, String) => Boolean =
      _ != 0 || _ != '0' || _ != ""

    // 需求二 : 定义一个函数  传入的参数是(0)('0')("") 返回false 否则返回true
    def f1(i: Int): Char => (String => Boolean) = {
      def f2(c: Char): String => Boolean = {
        def f3(s: String): Boolean = {
          i != 0 || c != '0' || s != ""
        }

        f3 _
      }

      f2 _
    }

    // 函数的化简
    def f11(i: Int): Char => (String => Boolean) = {
      (c: Char) => (s: String) => i != 0 || c != '0' || s != ""
    }

    println(f1(0)('0')(""))
    println(f1(1)('0')(""))
    println(f1(0)('1')(""))
    println(f1(0)('0')("1"))

    // 过度化简  可读性极差
    val function1: Int => Char => String => Boolean =
      i => c => i != 0 || c != '0' || _ != ""


    // 柯里化
    def func6(i:Int)(c:Char)(s:String):Boolean = {
      i != 0 || c != '0' || s != ""
    }

    val function2: Char => String => Boolean = func6(10)
    val function3: String => Boolean = function2('c')
    val bool: Boolean = function3("")

  }
}
