package chapter06

/**
 * @author yhm
 * @create 2021-09-16 14:26
 */
object Test14_TraitMixin {
  def main(args: Array[String]): Unit = {
    val student1 = new Student14
    println(student1.name1)
    println(student1.age1)

    println(student1.name)
    println(student1.age)

    //（4）动态混入：可灵活的扩展类的功能
    //（4.1）动态混入：创建对象时混入trait，而无需使类混入该trait
    //（4.2）如果混入的trait中有未实现的方法，则需要实现
    // 只有当前创建的一个对象具有混入的特质  类是没有的

    val teacher1: Teacher14 with Age14 = new Teacher14 with Age14 {
      override val name = "teacher"
      override var age = 18
    }

  }
}



trait Age14{
  val name:String
  var age:Int

  val name1:String = "age"
  var age1:Int = 10
}


abstract class Person14{
  val name:String = "person"
  var age:Int = 18

  val name1:String = "person1"
//  var age1:Int = 11
}

// 继承的时候 父类和特质不能有相同的具体属性  会发生冲突报错
// 报错的如果是val 常量 可以通过重写解决  如果是var 变量  只能去修改父类或者特质
// 如果继承的属性 一个是抽象的一个是非抽象的  不会发生冲突  需要注意var的属性不能重写
class Student14 extends Person14 with Age14{
  override val name: String = "student"
  age = 19

  // 通过重写解决
  override val name1: String = "student"
  age1 = 18
}


//（3）所有的Java接口都可以当做Scala特质使用
class Teacher14 extends java.io.Serializable{

}


