package com.ligx

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Created by ligx on 16/6/18.
  */
class Test extends FlatSpec with Matchers{

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
}
