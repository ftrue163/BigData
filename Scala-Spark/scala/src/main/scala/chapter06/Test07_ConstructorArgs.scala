package chapter06

/**
 * @author yhm
 * @create 2021-09-16 10:01
 */
object Test07_ConstructorArgs {
  def main(args: Array[String]): Unit = {
    val person0 = new Person07("zhangsan",11,"男")

    println(person0.name)

    println(person0.age)

    println(person0.sex)
  }

}

// 主构造器参数 分为3类:
// 没有修饰符 : 作为构造方法中的传入参数使用
// val 修饰 : 会自动生产同名的属性 并且定义为val
// var 修饰 : 会自动生产同名的属性 并且定义为var
class Person07 (name1:String,val age:Int,var sex:String){
  val name = name1
//  val age = age
//  var sex = sex
}
