package com.ligx.rpc

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.ligx.extensions.{CountExtension, MailboxExtension}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.Await

/**
  * Created by ligx on 16/6/12.
  */
class ClientActor extends Actor {

  var remoteActor: ActorSelection = _

  @scala.throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    remoteActor = context.actorSelection("akka.tcp://RpcServerSystem@127.0.0.1:2552/user/serverActor")
    println("远程服务端地址: " + remoteActor)
  }

  def receive: Receive = {
    case msg: AkkaMessage => {
      println("客户端发送: " + msg)
      println("客户端邮箱中的消息数量: " + MailboxExtension(context.system).getMailboxSize)
      remoteActor ! msg
    }
    case res: Response => {
      CountExtension(context.system).increment()
      println("客户端收到反馈: " + res)
    }
    case _ => println("客户端不支持的消息...")
  }
}

object ClientActor extends App{

  val clientSystem = ActorSystem("RpcClientSystem", ConfigFactory.load().getConfig("LocalConf"))
  val clientActor = clientSystem.actorOf(Props[ClientActor].withDispatcher("my-dispatcher"), "clientActor")

//  implicit val timeout = Timeout(5 seconds)

  val msgs = Array[AkkaMessage](AkkaMessage("msg1"), AkkaMessage("msg2"), AkkaMessage("msg3"))
  msgs foreach { msg => clientActor ! msg }

  Thread.sleep(4000)

  println("客户端收到消息的次数: " + CountExtension(clientSystem).getCount())
  clientSystem.terminate()
}
