package chapter08

/**
 * @author yhm
 * @create 2021-09-18 14:28
 */
object Test02_MatchValue {
  def main(args: Array[String]): Unit = {
    // 匹配常量
    def func1(x:Any):String = {
      x match {
        case 10 => "整数10"
        case 20.1 => "浮点数20.1"
        case 'x' => "字符x"
        case "hello" => "字符串hello"
        case _ => "其他数据"
      }
    }

    println(func1(10))
    println(func1(20.1))
    println(func1('x'))
    println(func1("hello"))
    println(func1(180))


    // 匹配类型
    def func2(x:Any):String ={
      x match {
        case i:Int => "整数"
        case c:Char => "字符"
        case s:String => "字符串"
        case _ => "其他"
      }
    }

    println(func2(1515))
    println(func2('\t'))
    println(func2("1515"))


  }
}
