package com.jx.dao.kafka



import com.jx.dao.sql.SqlUtilImpl
import com.jx.model.Datas.TableColumnMappingValue
import com.jx.util.{PropertiesUtils, StreamingContextUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, OffsetRange}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class KafkaOffsetUtil

/**
  *注意：
  * 1.如果kafka长时间没有进行消费 mysql里存的offset是7天以前的，会导致kafka消费失败
  * 2.如果新增kafka节点，那么读取kafka中的offset
  */

object KafkaOffsetUtil {

  private val logger: Logger = LoggerFactory.getLogger(classOf[KafkaOffsetUtil].getName)

  /**
    * 读取kafka的offset并且产生Dstream
    */
  def kafkaOffsetRead(topic:String,group:String): Unit ={
    val context: StreamingContext = StreamingContextUtil.getStreamingContext

    val properties = PropertiesUtils.apply()
    val kafkaBroker: String = properties.getProperty("jx.kafka.broker.list")
    val topics: Set[String] = Set(topic)

    val kafkaOffset: List[Map[String, Any]] = SqlUtilImpl.queryAll(s"select * from kafka_offset where topic=${topic} and group=${group}")

    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> kafkaBroker,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: Boolean),
      "auto.offset.reset" -> "earliest"
    )

    var mysqlOffset: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition,Long]()

    var kafkaDstream: InputDStream[ConsumerRecord[String, String]] = null

    if (kafkaOffset.size>0){
      for (map<-kafkaOffset){
        try{
          val partition: Int = map.get("partition").get.toString.toInt
          val offset: Int = map.get("offset").mkString.toInt
          val topicAndPartition: TopicPartition = new TopicPartition(topic,partition)
          mysqlOffset += topicAndPartition->offset
        }catch{
          case e:Exception=>logger.info(e.getMessage)
        }
      }
      kafkaDstream = KafkaUtils.createDirectStream(context,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe(topics,kafkaParams,mysqlOffset))
    }else{
      //mysql中无offset 故从broker中读取
      kafkaDstream = KafkaUtils.createDirectStream(context,PreferConsistent,Subscribe(topics,kafkaParams))

    }
  }

  /**
    * 保存kafka的offset到mysql
    * @param offsetRange
    */
  def saveOffset(offsetRange: Array[OffsetRange]): Unit ={
    for (offset<-offsetRange){
      val topic: String = offset.topic
      val partition: Int = offset.partition
      val offsetLong: Long = offset.untilOffset
      val sql = s"select * from kafka_offset where topic=${topic} and partition=${partition}"
      val offsetList: List[Map[String, Any]] = SqlUtilImpl.queryAll(sql)
      if (offsetList.size==0){
        TableColumnMappingValue("kafka_offset","offset",Array(Row(offsetLong)))
      }
    }
  }

  /**
    * 根据参数确定是否 保存kafka的offset到mysql
    * @param offsetRange
    */
  def saveOffset(offsetRange: Array[OffsetRange],flag:Boolean): Unit ={
    if (flag){
      this.saveOffset(offsetRange)
    }
  }


}
