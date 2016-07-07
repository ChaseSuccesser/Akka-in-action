package com.ligx.AkkaKafka

import java.util.Properties

import kafka.utils.Logging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

/**
  * Created by ligx on 16/7/7.
  */
case class AkkaProducerConfig(brokers: String = null,
                              clientId: String = null,
                              compress: Boolean = true,
                              batchSize: Int = 16384,
                              maxSendRetries: Int = 2,
                              acks: Integer = 1,
                              bufferMemoryBytes: Long = 10*1024*1024,
                              metadataFetchTimeoutMillis: Long = 1000
                             ){
  def validate = {
    require(brokers != null, "null brokers")
  }

  def toProperties = new Properties(){
    put("bootstrap.servers", brokers)
    put("buffer.memory", bufferMemoryBytes.toString)                // default=33554432
    put("retries", maxSendRetries.toString)                         // default=0
    put("acks", acks.toString)                                      // default=1
    put("compression.type", "gzip")                                 // default=none (none / gzip / snappy)
    put("batch.size", batchSize.toString)                           // default=16384
    put("key.serializer", classOf[StringSerializer].getName)
    put("value.serializer", classOf[ByteArraySerializer].getName)
    put("metadata.fetch.timeout.ms", metadataFetchTimeoutMillis.toString)
    put("block.on.buffer.full", "false")
    put("request.timeout.ms", 10000.toString)
    if (clientId != null) put("client.id", clientId)
  }
}

class AkkaProducer(config: AkkaProducerConfig) extends Logging with Callback{
  require(config != null, "null producer config")

  config.validate

  val producer = new KafkaProducer[String, Array[Byte]](config.toProperties)

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
