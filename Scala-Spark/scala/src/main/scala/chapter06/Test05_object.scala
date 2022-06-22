package chapter06

/**
 * @author yhm
 * @create 2021-09-16 9:17
 */
object Test05_object {
  def main(args: Array[String]): Unit = {
    // 创建对象
    var person0 = new Person05
    val person1 = new Person05

    // 引用对象的变量和常量表示其地址值能否发生变化
    person0 = new Person05
    //    person1 = new Person05

    // 引用对象的属性能否修改取决于属性在定义的时候是val还是var
    //    person1.name = "lisi"
    person1.age = 11


    // 自动类型推到无法使用多态
    val student0 = new Student05
    student0.sayHi()

    // 多态的写法
    // 父类的引用指向子类的实例
    val student01: Person05 = new Student05

    //    student01.sayHi()
  }

}


class Person05 {
  val name = "zhangsan"
  var age = 10
}

class Student05 extends Person05 {
  def sayHi(): Unit = {
    println("hi")
  }
}