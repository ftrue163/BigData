package chapter07

/**
 * @author yhm
 * @create 2021-09-17 10:03
 */
object Test04_List {
  def main(args: Array[String]): Unit = {

    //    （1）List默认为不可变集合
    val list: List[Any] = List(1,1,1, 1.0, "hello", 'c')
    val list3 = List(1, 2, 3, 4)

    //    （2）创建一个List（数据有顺序，可重复）
    //    （3）遍历List
    list.foreach(println)

    //    （4）List增加数据
    val list1: List[Any] = list :+ 1
    println(list1)

    val list2: List[Int] = 2 :: list3
    println(list2)

    val list5: List[Any] = list2 :: list3
    println(list5)


    //    （5）集合间合并：将一个整体拆成一个一个的个体，称为扁平化
    val list4: List[Int] = list2 ::: list3
    println(list4)

    //    （6）取指定数据
    val i: Int = list4(0)

    //    （7）空集合Nil
    val list6: List[Int] = 1 :: 2 :: 3 :: 4 :: Nil


  }
}
