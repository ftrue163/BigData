package chapter06

/**
 * @author yhm
 * @create 2021-09-16 10:07
 */
object Test08_Inherit {
  def main(args: Array[String]): Unit = {
    // 子类继承父类的属性和方法
    val student0 = new Student08

    println(student0.name)
    println(student0.age)
    student0.sayHi
    println("===========================")

    val student01 = new Student08("lisi")
  }
}


class Person08 () {
  println("调用父类的主构造器")

  var name  = "person"
  var age = 18
  def sayHi: Unit ={
    println("hi person")
  }


  def this(name:String){
    this()
    this.name = name
    println("调用父类的辅助构造器")
  }
}

// 子类继承
// scala中的继承本质上是继承一个父类的构造器
class Student08 (name:String) extends Person08 (name:String){
  println("调用子类的主构造器")

  def this(){
    this("zhangsan")
//    this.name = name
    println("调用子类的辅助构造器")
  }

}