package com.ligx.cluster.TwoPhaseCommit

/**
  * Created by Administrator on 2016/8/17.
  */
object Main {

  implicit class RichBoolean(b: Boolean) {
    def toOption[T](x: => T) = if (b) Some(x) else None
  }

  def main(args: Array[String]) {
    val list1 = List(2, 4, 6, 7)
    val list2 = List(1, 3, 5)

    val foo = list2 forall (_ % 2 == 1) toOption 3 getOrElse 4
    val result = list1 forall (_ % 2 == 0) toOption foo

//    for(v <- result; i <- 1 to 10) println(v + i)
    val result2 = for(v <- result) yield (v + 1)
    println(result2)
  }
}
