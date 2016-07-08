package com.ligx.AkkaKafka

import akka.actor.ActorSystem

/**
  * Created by ligx on 16/7/8.
  */
object Main extends App{

  val system = ActorSystem("AkkaSystem")

  val topicConfigs = Seq(
    TopicConfig(topic = "akka-topic", numConsumerThread = 1)
  )

  val zookeeper = "localhost:2181"
  val brokers = null
  val groupId = "akka-group"

  val kafkaActor = KafkaActor(system, zookeeper, brokers, groupId, topicConfigs, null, null)

  kafkaActor ! MessageReady
}
