package com.ligx.continuation

import scala.util.continuations._
/**
  * Created by ligx on 16/10/13.
  */
object demo extends App {

  val result = reset {
    shift { k: (Int=>Int) => k(7) } + 1
  }

  println(result)
}
