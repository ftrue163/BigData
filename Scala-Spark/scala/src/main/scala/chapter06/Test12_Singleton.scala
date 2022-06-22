package chapter06

/**
 * @author yhm
 * @create 2021-09-16 11:10
 */

object Test12_Singleton {
  def main(args: Array[String]): Unit = {
    val person: Person12 = Person12.getPerson
    val person1: Person12 = Person12.getPerson

    println(person.eq(person1))


    val student1: Student12 = Student12()
    val student2: Student12 = Student12()
    println(student1.eq(student2))

  }
}

// 单例模式
// 懒汉式: 优点是  开始的时候不占用内存  等到需要的时候再创建对象

class Person12 private(){

}

object Person12 {
  var person:Person12 = null

  def getPerson:Person12 = {
    if (person == null) {
      person = new Person12
    }
    person
  }
}


// 饿汉式: 优点是  最开始的时候直接创建对象  没有线程安全问题
class Student12 private(){

}

object Student12{
  val student:Student12 = new Student12

  def apply():Student12 = student

}
