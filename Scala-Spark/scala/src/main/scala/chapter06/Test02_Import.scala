package chapter06

// java的导包方式依然能够使用
//import java.util.ArrayList

/**
 * @author yhm
 * @create 2021-09-15 15:21
 */
object Test02_Import {
//  import java.util.ArrayList
  def main(args: Array[String]): Unit = {

    // 不导入包 直接写全类名也行
//    java.util.ArrayList

    // scala的导包可以在任何位置进行
    import java.util.ArrayList

    // 导入包下的所有类
    import java.util._
//    ArrayList
//    HashMap


  }
}
