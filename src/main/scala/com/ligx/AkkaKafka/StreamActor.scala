package com.ligx.AkkaKafka

import akka.actor.{Actor, ActorLogging}
import kafka.consumer.{ConsumerTimeoutException, KafkaStream}

/**
  * Created by ligx on 16/7/7.
  */
class StreamActor(stream: KafkaStream[Array[Byte], Array[Byte]]) extends Actor with ActorLogging{

  val streamIterator = stream.iterator()

  override def receive: Receive = {
    case RequestMessage =>
      try {
        if (streamIterator.hasNext()) {
          processMessage(streamIterator.next().message)
          self ! RequestMessage
        } else {
          log.info("KafkaStream中没有消息")
          notifySelf
        }
      } catch {
        case e: ConsumerTimeoutException => notifySelf  // Timeout exceptions are ok and should occur every "consumer.timeout.ms" millis
      }
    case NextMessage =>
      try {
        if (streamIterator.hasNext()) {
          self ! RequestMessage
        } else {
          log.info("KafkaStream中没有消息")
          notifySelf
        }
      } catch {
        case e: ConsumerTimeoutException => notifySelf
      }
  }

  def processMessage(msg: Array[Byte]) = {
    println("收到消息: " + new String(msg))
  }

  def notifySelf = self ! NextMessage
}
