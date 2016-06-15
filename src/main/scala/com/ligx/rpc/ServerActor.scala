package com.ligx.rpc

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * Created by ligx on 16/6/12.
  */

case class AkkaMessage(message: Any)
case class Response(message: Any)

class ServerActor extends Actor{

  def receive: Receive = {
    case msg: AkkaMessage => {
      println("服务端收到消息: " + msg.message)
      sender ! Response("response_" + msg.message)
    }
    case _ => println("服务端不支持的消息类型...")
  }
}

object ServerActor extends App{

  val system = ActorSystem("RpcServerSystem", ConfigFactory.load().getConfig("RemoteConf"))
  system.actorOf(Props[ServerActor], "serverActor")
}
