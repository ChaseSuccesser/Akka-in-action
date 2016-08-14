package com.ligx.AkkaKafka

import akka.actor.ActorSystem
import spray.json._

/**
  * Created by ligx on 16/7/8.
  */
object Main extends App{

  val system = ActorSystem("AkkaSystem")

  /* start consumer*/
  /*
  val topicConfigs = Seq(
//    TopicConfig(topic = "akka-topic", numConsumerThread = 1),
//    TopicConfig(topic = "akka-topic2", numConsumerThread = 1)
    TopicConfig(topic = "order", numConsumerThread = 1)
  )

  val kafkaActor = KafkaActor(system, topicConfigs)

  kafkaActor ! MessageReady
  */


  /*start producer*/
  import DefaultJsonProtocol._
  val producer = new AkkaProducer(system)
  val content = Map("strategy_type"->"pay", "order_id"->"11111", "action"->"hhaa").toJson.compactPrint
  producer.send("order", null, content)
  producer.close
  system.terminate()
}
