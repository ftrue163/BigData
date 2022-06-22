package chapter08

/**
 * @author yhm
 * @create 2021-09-18 15:12
 */
object Test05_MatchObject {
  def main(args: Array[String]): Unit = {
    val zhangsan = new Person05("zhangsan", 18)

    zhangsan match {
      case Person05("zhangsan",18) => println("找到张三啦")
      case _ => println("你不是zhangsan")
    }


  }
}


//class Person05 (val name:String,var age:Int){
//
//}
//
//object Person05{
//  // 创建对象的方法
//  def apply(name: String, age: Int): Person05 = new Person05(name, age)
//
//  // 解析对象的方法
//  def unapply(arg: Person05): Option[(String, Int)] = {
//    // 如果解析的参数为null
//    if (arg == null ) None else Some((arg.name,arg.age))
//  }
//}

// 样例类
// 里面的参数默认就是val的, 想要修改为变量 添加var即可
// 样例类中有大量的隐藏方法 都不需要自己去写
case class Person05(var name: String, age: Int)
