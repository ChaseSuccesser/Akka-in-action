package com.ligx.AkkaKafka

import akka.actor.ActorSystem

/**
  * Created by ligx on 16/7/8.
  */
object Main extends App{

  val system = ActorSystem("AkkaSystem")

  val topicConfigs = Seq(
//    TopicConfig(topic = "akka-topic", numConsumerThread = 1),
//    TopicConfig(topic = "akka-topic2", numConsumerThread = 1)
    TopicConfig(topic = "order", numConsumerThread = 1)
  )

  val kafkaActor = KafkaActor(system, topicConfigs)

  kafkaActor ! MessageReady
}
