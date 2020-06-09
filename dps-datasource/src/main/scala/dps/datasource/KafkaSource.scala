package dps.datasource

import scala.collection.mutable.Map

import org.apache.spark.SparkContext

import dps.datasource.define.DatasourceDefine
import org.apache.spark.storage.StorageLevel
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategy
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import scala.annotation.meta.param
import org.apache.spark.rdd.RDD
import dps.atomic.Operator
import dps.datasource.define.DatasourceParamDefine

class KafkaSource(override val sparkContext: SparkContext, override val params: Map[String, String],override val operator:Operator) extends StreamDatasource(sparkContext, params,operator) {
  override def read(variableKey:String) {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> params.get("bootstrapServers").get,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> params.get("bootstrapServers").get,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean))
    val topics = params.get("topics").get.split(",")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    
    stream.foreachRDD(records => {
      val streamRDD = records.map(record=>{
        val topic = record.topic()
        val partition = record.partition()
        Tuple3(topic,partition,record.value())
      })
      operator.setVariable(variableKey, streamRDD)
      operator.operation()
    })
  }
  def define(): DatasourceDefine = {
    val paramDefines = Map[String, DatasourceParamDefine](
      "bootstrapServers" -> new DatasourceParamDefine("Bootstrap Servers", "127.0.0.1:9002"),
      "group" -> new DatasourceParamDefine("Group Name", "group"),
      "topics" -> new DatasourceParamDefine("Topics", "topic"))
    val datasourceDefine = new DatasourceDefine("Kafka", paramDefines.toMap)
    datasourceDefine.id = "kafka_source_define"
    return datasourceDefine
  }
}