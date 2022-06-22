package chapter06

/**
 * @author yhm
 * @create 2021-09-16 10:27
 */
object Test10_InheritPro {
  def main(args: Array[String]): Unit = {
    val student1 = new Student10

    println(student1.name)
    println(student1.name1)
    println(student1.age)
    println(student1.age1)
    student1.sayHi()
    student1.sayHi1()


    //  如果使用多态
    val student11:Person10 = new Student10

    // 和java不同 即使是多态也会打印子类的属性
    println(student11.name1)

    student11.sayHi1()
  }
}


abstract class Person10 {
  // 抽象的属性
  val name: String
  var age: Int
  // 抽象的方法
  def sayHi():Unit

  // 非抽象的属性
  val name1:String = "person1"
  var age1:Int = 11
  // 非抽象的方法
  def sayHi1():Unit = {
    println("hi person")
  }
}


class Student10 extends Person10{

  // 抽象的属性和方法全部都要重写
  // 重写抽象的属性和方法  override关键字可以省略
  val name: String = "student"
  override var age: Int = 12

  override def sayHi(): Unit = {
    println("hi student")
  }

  // 非抽象的val常量可以重写
  override val name1: String = "student1"

  // 非抽象的var 变量不能重写   如果想要修改可以直接改
//  override var age1:Int = 18
  age1 = 18

  // 非抽象的方法也能重写
  override def sayHi1(): Unit = {
    println("hi student1")
  }
}
