package com.ligx

import com.ligx.restapi.commons.JsonUtil
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

import scala.collection.mutable

/**
  * Created by ligx on 16/6/18.
  */
class Test extends FlatSpec with Matchers{
  import DefaultJsonProtocol._
  import JsonUtil.AnyJsonFormat

  assertResult(3){
    3
  }

  "An empty set" should "have size 0" in {
    val stack = new mutable.Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be (2)
    stack.pop() should be (1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val enptyStack = new mutable.Stack[Int]
    a [NoSuchElementException] should be thrownBy {
      enptyStack.pop()
    }
  }

  "An map result" should "convert json String" in {
    val map = Map[String, Any]()
    val new_map = map + ("is_success" -> true, "count" -> 2)
    println(new_map.toJson.prettyPrint)
  }
}
