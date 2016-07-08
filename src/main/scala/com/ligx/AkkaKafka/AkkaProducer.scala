package com.ligx.AkkaKafka

import java.util.Properties
import scala.collection.JavaConverters._
import akka.actor.ActorSystem
import kafka.utils.Logging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

/**
  * Created by ligx on 16/7/7.
  */
object AkkaProducer{
  def toProps(system: ActorSystem) = {
    val config = system.settings.config.getConfig("kafka.producer")
    val kvSet = config.entrySet().asScala.map(entry => entry.getKey -> config.getString(entry.getKey))
    kvSet.foldLeft(new Properties()){
      case (p, (k, v)) => {
        p.put(k, v)
        p
      }
    }
  }
}

class AkkaProducer(system: ActorSystem) extends Logging with Callback{

  import AkkaProducer._

  val producer = new KafkaProducer[String, Array[Byte]](toProps(system))

  def send(topic: String, key: String, value: Array[Byte]) = {
    producer.send(new ProducerRecord[String, Array[Byte]](topic, key, value))
  }

  override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
    if(e != null){
      logger.error(e.getMessage)
    }
  }

  def close = {
    producer.close()
  }
}
