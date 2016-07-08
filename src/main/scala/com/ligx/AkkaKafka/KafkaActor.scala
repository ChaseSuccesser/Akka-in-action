package com.ligx.AkkaKafka

import scala.collection.JavaConverters._
import akka.actor.{Actor, ActorSystem, Props}

/**
  * Created by ligx on 16/7/7.
  */
sealed trait KafkaStreamDriverMessage

case object MessageReady extends KafkaStreamDriverMessage

case object RequestMessage extends KafkaStreamDriverMessage

case object NextMessage extends KafkaStreamDriverMessage

case class TopicConfig(topic: String, numConsumerThread: Int)

object KafkaActor{
  def apply(actorSystem: ActorSystem,
            zookeeper: String,
            brokers: String,
            groupId: String,
            topicConfigs: Seq[TopicConfig],
            consumerConfig: AkkaConsumerConfig = null,
            producerConfig: AkkaProducerConfig = null) = {
    val props = Props(new KafkaActor(zookeeper, brokers, groupId, topicConfigs, consumerConfig, producerConfig))
    actorSystem.actorOf(props, "KafkaActor")
  }
}

class KafkaActor(zookeeper: String,
                 brokers: String,
                 groupId: String,
                 topicConfigs: Seq[TopicConfig],
                 consumerConfig: AkkaConsumerConfig = null,
                 producerConfig: AkkaProducerConfig = null) extends Actor {

  val akkaConsumer = new AkkaConsumer(if(consumerConfig==null) AkkaConsumerConfig(zookeeper, groupId) else consumerConfig.copy(zookeeper, groupId))

  val topicStreams = akkaConsumer.createMessageStreams(topicConfigs)

  val topicActors = for(topicConfig <- topicConfigs) yield{
    val props = Props(classOf[TopicActor], topicConfig, topicStreams.get(topicConfig.topic).asScala)
    context.actorOf(props, s"kafka-${topicConfig.topic}")
  }

  override def receive: Receive = {
    case MessageReady => notifyTopicerMessageReady
  }


  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    akkaConsumer.close
  }

  def notifyTopicerMessageReady = topicActors.foreach(_ ! MessageReady)
}
