package com.ligx.AkkaKafka

import akka.actor.{Actor, ActorLogging}
import kafka.consumer.{ConsumerTimeoutException, KafkaStream}
import kafka.message.MessageAndMetadata

/**
  * Created by ligx on 16/7/7.
  */
class StreamActor(stream: KafkaStream[Array[Byte], Array[Byte]]) extends Actor with ActorLogging{

  val streamIterator = stream.iterator()

  override def receive: Receive = {
    case RequestMessage =>
      try {
        if (streamIterator.hasNext()) {
          processMessage(streamIterator.next())
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

  def processMessage(result: MessageAndMetadata[Array[Byte], Array[Byte]]) = {
    println(s"topic: ${result.topic}, partition: ${result.partition}, offset: ${result.offset}, message: " + new String(result.message()))
  }

  def notifySelf = self ! NextMessage
}
