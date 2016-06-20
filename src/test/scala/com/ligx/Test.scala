package com.ligx

/**
  * Created by ligx on 16/6/18.
  */
object Test extends App{

  val seq = List("a")
  seq.view.map(s => s.charAt(0).toUpper)
}
