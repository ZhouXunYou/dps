package dps.datasource

import dps.atomic.Operator
import dps.datasource.define.{DatasourceDefine, DatasourceParamDefine}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection.mutable.Map
import org.apache.spark.SparkConf

class KafkaSource(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val params: Map[String, String],override val operator:Operator) extends StreamDatasource(sparkSession, sparkConf,params,operator) {
  override def read(variableKey:String) {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> params.get("bootstrapServers").get,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> params.get("group").get,
//      "auto.offset.reset" -> "latest",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean))
    val topics = params.get("topics").get.split(",")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    
    stream.foreachRDD(records => {
      if(!records.isEmpty()){
        val streamRDD = records.map(record=>{
          val topic = record.topic()
          val partition = record.partition()
          (topic,partition,record.value())
        })
        operator.setVariable(variableKey, streamRDD)
        operator.operation()
      }
    })
  }
  def define(): DatasourceDefine = {
    val paramDefines = Map[String, DatasourceParamDefine](
      "bootstrapServers" -> new DatasourceParamDefine("Bootstrap Servers", "127.0.0.1:9092"),
      "group" -> new DatasourceParamDefine("Group Name", "group"),
      "topics" -> new DatasourceParamDefine("Topics", "topic"))
    val datasourceDefine = new DatasourceDefine("Kafka", paramDefines.toMap)
    datasourceDefine.id = "kafka_source_define"
    return datasourceDefine
  }
}