package com.ligx.util

import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Author: ligongxing.
  * Date: 2016年12月01日17时40分.
  */
object FutureDemo extends App{

  val executorService = Executors.newFixedThreadPool(10)
  implicit val executorContext = ExecutionContext.fromExecutor(executorService)

  /**
    * 按照future完成的顺序组装结果
    * 如果有任何一个future fail，最终结果也是只包含exception的future
    *
    * 如果想按顺序等future的结果（非完成顺序），可以把多个future放到for expression中
    * @param futures
    * @param values
    * @param prom
    * @return
    */
  def processFutures(futures: List[Future[Any]], values: List[Any], prom: Promise[List[Any]]): Future[List[Any]] = {
    val future =
      if(futures.length == 1)
        futures.head
      else
        Future.firstCompletedOf(futures)

    future onComplete{
      case Success(value) if futures.length == 1 => Promise.successful(value :: values)
      case Success(value) => {
        val newList = futures diff List(value)
        println(newList.length)
        processFutures(newList, value :: values, prom)
      }
      case Failure(ex) => prom.failure(ex)
    }

    prom.future
  }


  val future1 = Future{Thread.sleep(3000); 1}
//  val future2 = Promise.failed(new RuntimeException("boom")).future
  val future2 = Future{Thread.sleep(2000); 2}
  val future3 = Future{Thread.sleep(1000); 3}


  val result = processFutures(List(future1, future2, future3), List(), Promise[List[Any]]())
  result onComplete{
    case Success(value) => {
      value foreach println
    }
    case Failure(ex) => {
      println(ex.getMessage)
    }
  }


//  val result = for{
//    f1 <- future1
//    f2 <- future2
//    f3 <- future3
//  } yield (f1, f2, f3)
//
//  result onComplete {
//    case Success(value) => value.productIterator.foreach(println)
//    case Failure(ex) => println(ex.getMessage)
//  }
}
