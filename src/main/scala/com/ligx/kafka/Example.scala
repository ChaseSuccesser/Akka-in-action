package com.ligx.kafka

import akka.actor.{Actor, ActorSystem, Props}
import kafka.serializer.DefaultDecoder

/**
  * Created by Administrator on 2016/7/1.
  */
object Example extends App{

  class Printer extends Actor{
    def receive = {
      case x: Any => {
        println(x)
        sender ! StreamFSM.Processed
      }
    }
  }

  val system = ActorSystem("test")
  val printer = system.actorOf(Props[Printer], "printer")

  val consumerProps = AkkaConsumerProps.forSystem(
    system = system,
    zkConnect = "localhost:2181",
    topic = "test",
    group = "group-2",
    streams = 4, //one per partition
    keyDecoder = new DefaultDecoder(),
    msgDecoder = new DefaultDecoder(),
    receiver = printer
  )

  val consumer = new AkkaConsumer(consumerProps)
  consumer.start()
}
