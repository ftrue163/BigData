package chapter06

/**
 * @author yhm
 * @create 2021-09-16 10:18
 */
object Test09_AbsClass {
  def main(args: Array[String]): Unit = {
    val person01:Person09 = new Student09

    person01.sayHi()

    // 匿名必然是多态的用法
    val person0: Person09 = new Person09 {
      override val name: String = "匿名子类"
      override var age: Int = 10

      // 匿名子类中不要定义多余的东西 外部无法调用
      val sex:String = "nan"

      override def sayHi(): Unit = {
        println("hi 匿名子类")
      }
    }


  }
}

// 抽象的属性只能存在于抽象的类中
// 抽象类的使用有两种方法 :
// 使用子类继承抽象类
// 使用匿名子类
abstract class Person09{
  val name:String
  var age:Int

  def sayHi():Unit
}


class Student09 extends Person09{

  override val name: String = "student"
  override var age: Int = 18

  override def sayHi(): Unit = {
    println("hi student")
  }
}