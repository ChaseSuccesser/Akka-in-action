package com.ligx.kafka

import java.util.Properties

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import akka.actor.{ActorContext, ActorRef, ActorSystem}
import akka.util.Timeout
import kafka.consumer.{ConsumerConfig, TopicFilter}
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

/**
  * Created by ligx on 16/6/17.
  */
object AkkaConsumer{
  def toProps(props: collection.mutable.Set[(String, String)]): Properties = {
    props.foldLeft(new Properties()){
      case (p, (k, v)) => {
        p.setProperty(k, v)
        p
      }
    }
  }


}

class AkkaConsumer[Key, Msg](props: AkkaConsumerProps[Key, Msg]) {
  import AkkaConsumer._

  /**
    * 根据zkConnect和groupId 以及配置文件中的kafka.consumer配置得到创建ConsumerConfig所需的Properties
    * @param zkConnect
    * @param groupId
    * @return
    */
  def kafkaConsumerProps(zkConnect: String, groupId: String) = {
    val consumerConfig = props.system.settings.config.getConfig("kafka.consumer")
    val consumerProps = consumerConfig.entrySet().asScala.map{
      entry => entry.asInstanceOf[String] -> consumerConfig.getValue(entry.getKey).asInstanceOf[String]
    } ++ Set("zookeeper.connect" -> zkConnect, "group.id" -> groupId)
    toProps(consumerProps)
  }

  def kafkaConsumer(zkConnect: String, groupId: String) = {
    Consumer.create(new ConsumerConfig(kafkaConsumerProps(zkConnect, groupId)))
  }

  def createConnection(props: AkkaConsumerProps[Key, Msg]) = {
    import props._
    val consumerConfig = new ConsumerConfig(kafkaConsumerProps(zkConnect, group))
    val consumerConnector = Consumer.create(consumerConfig)
    connectorActorName.map(name => actorRefFactory.actorOf())
  }
}

object AkkaConsumerProps {
  def forSystem[Key, Msg](system: ActorSystem,
                          zkConnect: String,
                          topic: String,
                          group: String,
                          streams: Int,
                          keyDecoder: Decoder[Key],
                          msgDecoder: Decoder[Msg],
                          receiver: ActorRef,
                          msgHandler: (MessageAndMetadata[Key, Msg]) => Any = defaultHandler[Key, Msg],
                          connectorActorName: Option[String] = None,
                          maxInFlightPerStream: Int = 64,
                          startTimeout: Timeout = Timeout(5 seconds),
                          commitConfig: CommitConfig = CommitConfig()): AkkaConsumerProps[Key, Msg] =
    AkkaConsumerProps(system, system, zkConnect, Right(topic), group, streams, keyDecoder, msgDecoder, msgHandler, receiver, connectorActorName, maxInFlightPerStream, startTimeout, commitConfig)

  def forSystemWithFilter[Key, Msg](system: ActorSystem,
                                    zkConnect: String,
                                    topicFilter: TopicFilter,
                                    group: String,
                                    streams: Int,
                                    keyDecoder: Decoder[Key],
                                    msgDecoder: Decoder[Msg],
                                    receiver: ActorRef,
                                    msgHandler: (MessageAndMetadata[Key,Msg]) => Any = defaultHandler[Key, Msg],
                                    connectorActorName:Option[String] = None,
                                    maxInFlightPerStream: Int = 64,
                                    startTimeout: Timeout = Timeout(5 seconds),
                                    commitConfig: CommitConfig = CommitConfig()): AkkaConsumerProps[Key, Msg] =
    AkkaConsumerProps(system, system, zkConnect, Left(topicFilter), group, streams, keyDecoder, msgDecoder, msgHandler, receiver, connectorActorName, maxInFlightPerStream, startTimeout, commitConfig)

  def forContext[Key, Msg](context: ActorContext,
                           zkConnect: String,
                           topic: String,
                           group: String,
                           streams: Int,
                           keyDecoder: Decoder[Key],
                           msgDecoder: Decoder[Msg],
                           receiver: ActorRef,
                           msgHandler: (MessageAndMetadata[Key,Msg]) => Any = defaultHandler[Key, Msg],
                           connectorActorName:Option[String] = None,
                           maxInFlightPerStream: Int = 64,
                           startTimeout: Timeout = Timeout(5 seconds),
                           commitConfig: CommitConfig): AkkaConsumerProps[Key, Msg] =
    AkkaConsumerProps(context.system, context, zkConnect, Right(topic), group, streams, keyDecoder, msgDecoder, msgHandler, receiver,connectorActorName, maxInFlightPerStream, startTimeout, commitConfig)

  def forContextWithFilter[Key, Msg](context: ActorContext,
                                     zkConnect: String,
                                     topicFilter: TopicFilter,
                                     group: String,
                                     streams: Int,
                                     keyDecoder: Decoder[Key],
                                     msgDecoder: Decoder[Msg],
                                     receiver: ActorRef,
                                     msgHandler: (MessageAndMetadata[Key,Msg]) => Any = defaultHandler[Key, Msg],
                                     connectorActorName:Option[String] = None,
                                     maxInFlightPerStream: Int = 64,
                                     startTimeout: Timeout = Timeout(5 seconds),
                                     commitConfig: CommitConfig): AkkaConsumerProps[Key, Msg] =
    AkkaConsumerProps(context.system, context, zkConnect, Left(topicFilter), group, streams, keyDecoder, msgDecoder, msgHandler, receiver,connectorActorName, maxInFlightPerStream, startTimeout, commitConfig)

  def defaultHandler[Key, Msg]: (MessageAndMetadata[Key, Msg]) => Any = msg => msg.message()
}

// TODO TopicFilter  MessageAndMetadata
case class AkkaConsumerProps[Key, Msg](system: ActorSystem,
                                       actorRefFactory: ActorRefFactory,
                                       zkConnect: String,
                                       topicFilterOrTopic: Either[TopicFilter, String],
                                       group: String,
                                       streams: Int,
                                       keyDecoder: Decoder[Key],
                                       msgDecoder: Decoder[Msg],
                                       msgHandler: (MessageAndMetadata[Key, Msg]) => Any,
                                       receiver: ActorRef,
                                       connectorActorName: Option[String],
                                       maxInFlightPerStream: Int = 64,
                                       startTimeout: Timeout = Timeout(5 seconds),
                                       commitConfig: CommitConfig = CommitConfig())

case class CommitConfig(commitInterval: Option[FiniteDuration] = Some(10 seconds),
                        commitAfterMsgCount: Option[Int] = Some(10000),
                        commitTimeout: Timeout = Timeout(5 seconds))
