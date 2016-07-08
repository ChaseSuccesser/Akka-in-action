package com.ligx.AkkaKafka

import java.util.Properties

import akka.actor.ActorSystem

import scala.collection.JavaConverters._
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}

/**
  * Created by ligx on 16/7/7.
  */
object AkkaConsumer{

  def toProps(system: ActorSystem) = {
    val config = system.settings.config.getConfig("kafka.consumer")
    require(config!=null, "null kafka.consumer config")
    val kvSet = config.entrySet().asScala.map(entry => entry.getKey -> config.getString(entry.getKey))
    kvSet.foldLeft(new Properties()){
      case (p, (k, v)) => {
        p.put(k, v)
        p
      }
    }
  }
}

class AkkaConsumer(system: ActorSystem) {

  import AkkaConsumer._

  val consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(toProps(system)))

  def createMessageStreams(topicConfigs: Seq[TopicConfig]) = {
    consumer.createMessageStreams(topicConfigs.map(topicConfig => (topicConfig.topic, topicConfig.numConsumerThread.asInstanceOf[Integer])).toMap.asJava)
  }

  def close = {
    consumer.commitOffsets()
    consumer.shutdown()
  }
}
