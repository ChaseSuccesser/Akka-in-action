package com.ligx.AkkaKafka

import akka.actor.ActorSystem
import spray.json._

/**
  * Created by ligx on 16/7/8.
  */
object Main extends App{

  val system = ActorSystem("AkkaSystem")

  /* consumer start */
  val topicConfigs = Seq(
    TopicConfig(topic = "order", numConsumerThread = 1)
  )

  val consumerActor = ConsumerActor(system, topicConfigs)
  consumerActor ! MessageReady
  /* consumer end */


  /* producer start */
//  import DefaultJsonProtocol._
//  import com.ligx.restapi.commons.JsonUtil._
//  val producer = new AkkaProducer(system)
//
//  val content = Map("strategy_type"->"sync_order", "order_id"->18559304836477l, "action"->"create_order").toJson.compactPrint
//  val appId = 23
//  val sign = "J8@dC!3g"
//  val message = s"$appId $sign CUSTOM $content"
//
//  producer.send("order", null, message)
//  producer.close
//  system.terminate()
  /* producer end */
}
