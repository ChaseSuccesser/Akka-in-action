package com.ligx.AkkaKafka

import akka.actor.Actor
import kafka.consumer.KafkaStream

/**
  * Created by ligx on 16/7/7.
  */
class StreamActor(stream: KafkaStream[Array[Byte], Array[Byte]]) extends Actor{

  val streamIterator = stream.iterator()

  override def receive: Receive = {
    case RequestMessage =>
      if(streamIterator.hasNext()){
        processMessage(streamIterator.next().message)
        self ! RequestMessage
      } else{
        self ! NextMessage
      }
    case NextMessage =>
      if(streamIterator.hasNext()){
        self ! RequestMessage
      }else{
        self ! NextMessage
      }
  }

  def processMessage(msg: Array[Byte]) = {
    println(msg.asInstanceOf[String])
  }
}
