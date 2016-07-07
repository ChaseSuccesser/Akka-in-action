package com.ligx.AkkaKafka

import java.util.Properties

import scala.collection.JavaConverters._

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}

/**
  * Created by ligx on 16/7/7.
  */
case class AkkaConsumerConfig(
                           zookeeper: String = null,
                           groupId : String = null,
                           autoOffsetResetToStart: Boolean = false,
                           consumerTimeoutMillis: Long = 500,
                           autoCommitIntervalMillis: Long = 10000
                         ){
  def validate = {
    require(zookeeper != null, "null zookeeper")
    require(groupId != null, "null groupId")
  }

  def toProperties = new Properties(){
    put("zookeeper.connect", zookeeper)
    put("group.id", groupId)
    put("zookeeper.session.timeout.ms", "5000")                          // ZooKeeper session timeout. If the consumer fails to heartbeat to ZooKeeper for this period of time it is considered dead and a rebalance will occur.
    put("zookeeper.sync.time.ms", "200")                                 // How far a ZK follower can be behind a ZK leader
    put("auto.commit.interval.ms", autoCommitIntervalMillis.toString)
    put("consumer.timeout.ms", consumerTimeoutMillis.toString)           // Throw a timeout exception to the consumer if no message is available for consumption after the specified interval (default = -1 - blocking)
    put("auto.offset.reset", if (autoOffsetResetToStart) "smallest" else "largest")
  }
}

class AkkaConsumer(config: AkkaConsumerConfig) {
  require(config != null, "null consumer config")

  config.validate

  val consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(config.toProperties))

  def createMessageStreams(topicConfigs: Seq[TopicConfig]) = {
    consumer.createMessageStreams(topicConfigs.map(topicConfig => (topicConfig.topic, topicConfig.numConsumerThread.asInstanceOf[Integer])).toMap.asJava)
  }

  def close = {
    consumer.commitOffsets()
    consumer.shutdown()
  }
}
