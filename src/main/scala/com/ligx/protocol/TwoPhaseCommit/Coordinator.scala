package com.ligx.protocol.TwoPhaseCommit

import akka.actor.{Actor, PoisonPill, Props}
import akka.pattern.ask
import com.ligx.protocol.TwoPhaseCommit.Helper._
import scala.reflect.ClassTag

/**
  * Created by ligx on 16/8/15.
  */
abstract class Coordinator[T, TA: ClassTag] extends Actor {

  import context.dispatcher

  private def transactor(id: String) = context.child(id) getOrElse context.actorOf(Props(implicitly[ClassTag[TA]].runtimeClass), id)

  final def receive = {
    case rs: ReqSeq[T] => import rs.askTimeout; transactor(rs.tid) ? Process(rs) foreach (sender ! _)
  }
}

abstract class Transactor[T, P <: Processor[T] : ClassTag] extends TransactorLike[T] {
  def process(id: String) = context.child(id) getOrElse context.actorOf(Props(implicitly[ClassTag[P]].runtimeClass), id)

  final def receive = {
    case p: Process[T] =>
      // 调用default样本类的构造函数,创建default样本类实例,而default是Merging的子类,其实也是得到了Merging类的实例
      scheduleTimeouts(parent == null, p.rs.merging(acc, votes))
      transact = p.rs
      parent = sender
      for ((r, i) <- p.rs.data.zipWithIndex) process(p.rs.tid) ! Req(tid, r, acc.size + i) // 1 向参与者发送事务请求
      acc ++= p.rs.data
    case v: Vote[T] =>
      stat += sender -> v
      /*
      Merging类的apply方法的逻辑：
      如果没有收到全部参与者的响应,就返回None
      如果收到了全部参与者返回的响应
        如果收到的全部的参与者返回的响应中,有表示失败的响应,就返回Rollback[T] _
        如果收到的全部的参与者返回的响应中，都表示执行事务成功，则返回Commit[T] _
     */
      for (newVote <- transact.merging(acc, votes).apply; (actor, vote) <- stat) actor ! newVote(vote.req)
    case a: Ack[T] =>
      stat -= stat.find(_._2.req == a.vote.req).get
      if (stat.isEmpty) {
        a.vote.isCommit toOption commit getOrElse rollback
        self ! PoisonPill
      }
    case "Timeout" =>
      // 如果超时,并且收到了一些参与者发回的Vote,则对这些参与者发送rollback请求
      if (stat.nonEmpty) {
        for ((actor, vote) <- stat) actor ! Rollback(vote.req)
        rollback
      }
      self ! PoisonPill
  }
}

trait Processor[T] extends ProcessorLike[T] {

  import context.dispatcher

  final def receive = {
    case r: Req[T] => process(r) foreach (sender ! _) // 2 参与者收到Req，执行事务，向协调者发送Vote yes/no
    case o: Commit[T] => complete(o.req) map (_ => Ack(o)) foreach (sender ! _)
    case o: Rollback[T] => rollback(o.req) map (_ => Ack(o)) foreach (sender ! _)
  }
}