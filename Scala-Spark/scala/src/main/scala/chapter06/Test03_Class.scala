package chapter06

import scala.beans.BeanProperty

/**
 * @author yhm
 * @create 2021-09-15 15:35
 */
object Test03_Class {
  def main(args: Array[String]): Unit = {
    val person0 = new Person03

    person0.getName

    person0.setAge(11)
  }
}


class Person03{
  // 属性的语法
  // scala当中已经使用val 和var 来区分属性的读写权限了  所有不再需要javaBean的写法
  @BeanProperty
  val name = "zhangsan"

  // 如果多行都支持java bean的语法, 需要单独设置
  @BeanProperty
  var age = 10




}