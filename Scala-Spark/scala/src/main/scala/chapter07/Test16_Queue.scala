package chapter07

import scala.collection.immutable.Queue
import scala.collection.mutable

/**
 * @author yhm
 * @create 2021-09-18 14:09
 */
object Test16_Queue {
  def main(args: Array[String]): Unit = {
    //不可变队列
    val queue: Queue[Int] = Queue(1, 2, 3, 4)

    // 方法的调用
    val queue1: Queue[Int] = queue.enqueue(5)
    println(queue1)

    val dequeue: (Int, Queue[Int]) = queue.dequeue
    println(dequeue)

    // 可变队列queue
    // 和java中的队列一致 推荐使用
    val queue2: mutable.Queue[Int] = mutable.Queue(1, 2, 3, 4)

    // 方法调用
    queue2.enqueue(5)
    println(queue2)

    val i: Int = queue2.dequeue()
    println(queue2)
  }
}
