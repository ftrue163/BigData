package chapter06

/**
 * @author yhm
 * @create 2021-09-16 14:19
 */
object Test13_Trait {
  def main(args: Array[String]): Unit = {
    val age1: Age13 = new Age13 {
      override val age: Int = 14

      override def sayHi(): Unit = {
        println("hi 匿名子类")
      }
    }


  }
}


// 创建一个特质
// 两种用法 : 使用子类继承; 使用匿名子类

trait Age13 {
  // 允许出现抽象的属性和方法
  val age :Int
  def sayHi():Unit

  // 允许出现非抽象的属性
  val age1:Int = 10
  def sayHi1()={
    println("hi ")
  }
}

class Person13 {

}
// 如果有父类
class Student13 extends Person13 with Age13{
  override val age: Int = 12

  override def sayHi(): Unit = {}
}

// 如果没有父类
class Student131 extends Age13 {
  override val age: Int = 13

  override def sayHi(): Unit = {}
}


