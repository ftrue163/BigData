package chapter06

/**
 * @author yhm
 * @create 2021-09-15 15:05
 */
object Test01_Package {
  def main(args: Array[String]): Unit = {
    // 包对象的调用
    println(packageName)

    sayHello()
  }
}


// scala独有的包定义语法
// 不推荐使用 因为内部写的包名和文件夹的名称无法对应  会导致特别难找
package com{

  import chapter06.com.atguigu.Inner

  object Outer{
    def main(args: Array[String]): Unit = {
      println("hello outer")

      // 外层调用内存  要避免两个方法的互相调用
      Inner.main(args)
    }
  }

  package atguigu{
    object Inner{
      def main(args: Array[String]): Unit = {
        println("hello inner")


        println(packageName)
        sayHello()
        // 内存对象调用外层对象
//        Outer.main(args)
      }
    }
  }
}
// scala独有的包定义语法 下的包对象
package object com{
  val packageName = "com"
  def sayHello():Unit = {
    println("hello com")
  }
}
