package chapter07

/**
 * @author yhm
 * @create 2021-09-17 15:02
 */
object Test12_HighFunc {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val nestedList: List[List[Int]] = List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))
    val wordList: List[String] = List("hello world", "hello atguigu", "hello scala")

    //    （1）过滤
    //    遍历一个集合并从中获取满足指定条件的元素组成一个新的集合
    // 过滤出大于等于5的数
    val list1: List[Int] = list.filter((i: Int) => i >= 5)
    println(list1)

    // 过滤出偶数
    val list2: List[Int] = list.filter((i: Int) => i % 2 == 0)
    println(list2)

    val list3: List[Int] = list.filter(_ % 2 == 0)
    println(list3)

    //    （2）转化/映射（map）
    //    将集合中的每一个元素映射到某一个函数


    // 将元素  * 2
    // map会将集合的数据类型发生变化  变为匿名函数的返回值类型
    val list4: List[Double] = list.map((i: Int) => i * 2.0)
    println(list4)

    // 将整数值  i => 二元组  ("我是",i)
    val tuples: List[(String, Int)] = list.map((i: Int) => new Tuple2[String, Int]("我是", i))
    println(tuples)

    val tuples1: List[(String, Int)] = list.map(("我是", _))
    println(tuples1)


    //    （3）扁平化
    val flatten: List[Int] = nestedList.flatten
    println(flatten)

    // flatten的扁平化只能用于特殊的集合结构
    //    list.flatten

    //    （4）扁平化+映射 注：flatMap相当于先进行map操作，在进行flatten操作
    //    集合中的每个元素的子元素映射到某个函数并返回新集合
    val flatten1: List[Char] = wordList.flatten
    println(flatten1)

    // 将元素  str => list(单词,单词)
    val list5: List[List[String]] = wordList.map(
      (s: String) => {
        val strings: Array[String] = s.split(" ")
        strings.toList
      }
    )

    val flatten2: List[String] = list5.flatten
    println(flatten2)

    // flatten只能针对特殊的格式进行扁平化   不是特殊的格式需要先进行map 再进行flatten
    // 可以使用flatMap来代替两个函数
    val strings1: List[String] = wordList.flatMap(
      (s: String) => {
        val strings: Array[String] = s.split(" ")
        strings.toList
      })

    println(strings1)


    val strings: List[String] = wordList.flatMap(_.split(" "))
    println(strings)



    //    （5）分组(group)
    //    按照指定的规则对集合的元素进行分组
    // 结果会变成一个map  map是无序不可排序 key是不可以重复的

    // 返回值 是以分组的结果判断进入哪个分组  map(分组的结果,List(进入该组的元素))
    // 以自身进行分组
    val map: Map[String, List[String]] = strings.groupBy((s: String) => s)

    strings.groupBy(s => s)
    println(map)


    val map1: Map[Int, List[Int]] = list.groupBy((i: Int) => i % 2)

    list.groupBy(_ % 2)

    println(map1)

    val map2: Map[Boolean, List[Int]] = list.groupBy(_ % 2 == 0)
    println(map2)


    //    （6）简化（归约）
    val list6 = List(1, 2, 3, 4, 5, 6)

    // 相加
    val i: Int = list6.reduce((res: Int, elem: Int) => res + elem)
    println(i)

    // 相减
    // reduce的方法
    // 会把第一个元素当做初始值
    // 元素的类型和结果的类型必须一致

    // 底层调用的是reduceLeft
    val i1: Int = list6.reduce((res: Int, elem: Int) => res - elem)
    println(i1)

    // reduceLeft要求结果类型和元素类型也是一致的
    val d: Int = list6.reduceLeft((res: Int, elem: Int) => res - elem)
    println(d)


    //  6 - 5 - 4 - 3 - 2 - 1
    // reduceRight的计算逻辑
    // 1 - (2 - (3 - (4 - (5 - 6))))
    val i2: Int = list6.reduceRight((res: Int, elem: Int) => res - elem)
    println(i2)


    //    （7）折叠
    // fold可以手动填写初始值  不再使用第一个元素作为初始值
    // 依然要求结果的数据类型和元素的类型保持一致
    val i3: Int = list6.fold(0)((res: Int, elem: Int) => res - elem)
    println(i3)

    // 归约: ("总和是",sum)
    // foldLeft支持填写初始值 以及改变结果值的数据类型
    val tuple: (String, Int) = list6.foldLeft(("总和是", 0))((res: (String, Int), elem: Int) => ("总和是", res._2 + elem))
    println(tuple)


    // foldRight
    // 10 - (6 - (5 - (4 - (3 -(2 -1)))))
    val i4: Int = list6.foldRight(10)((res: Int, elem: Int) => res - elem)
    println(i4)


  }
}
