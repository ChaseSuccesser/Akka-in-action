package com.ligx.AkkaKafka

import akka.actor.{Actor, ActorSystem, Props}

import scala.collection.JavaConverters._

/**
  * Created by Administrator on 2016/8/15.
  */
sealed trait KafkaStreamDriverMessage

case object MessageReady extends KafkaStreamDriverMessage

case object RequestMessage extends KafkaStreamDriverMessage

case object NextMessage extends KafkaStreamDriverMessage

case class TopicConfig(topic: String, numConsumerThread: Int)

object ConsumerActor {
  def apply(system: ActorSystem, topicConfigs: Seq[TopicConfig]) = {
    val props = Props(classOf[ConsumerActor], system, topicConfigs)
    system.actorOf(props)
  }
}

class ConsumerActor private (system: ActorSystem, topicConfigs: Seq[TopicConfig]) extends Actor {

  val akkaConsumer = new AkkaConsumer(system)

  val topicStreams = akkaConsumer.createMessageStreams(topicConfigs)

  /*
                  ConsumerActor
                  /           \
              TopicActor   TopicActor  .....
            /           \
      StreamActor    StreamActor  .......
   */
  val topicActors = for(topicConfig <- topicConfigs) yield {
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
