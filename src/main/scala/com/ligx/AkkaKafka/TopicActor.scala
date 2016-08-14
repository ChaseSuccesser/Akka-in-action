package com.ligx.AkkaKafka

import akka.actor.{Actor, Props}
import kafka.consumer.KafkaStream

/**
  * Created by ligx on 16/7/7.
  */
class TopicActor(topicConfig: TopicConfig, streams: Seq[KafkaStream[Array[Byte], Array[Byte]]]) extends Actor {

  val workers = for(i <- 1 to topicConfig.numConsumerThread) yield {
    context.actorOf(Props(classOf[StreamActor], streams(i-1)), s"kafka-${topicConfig.topic}-$i")
  }

  override def receive: Receive = {
    case MessageReady => workers.foreach(_ ! RequestMessage)
  }
}
