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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ContextCleaner

class KafkaSource(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val operator:Operator) extends StreamDatasource(sparkSession, sparkConf,operator) {
  override def read() {
      
//      sparkConf.get("bootstrapServers")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> sparkConf.get("kafka.bootstrapServers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> sparkConf.get("kafka.group"),
      "auto.offset.reset" -> "latest",
//      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean))
    val topics = sparkConf.get("kafka.topics").split(",")
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
        operator.setVariable(sparkConf.get("kafka.ds.name"), streamRDD);
        operator.operation()
//        operator.missionVariables.foreach(kv=>{
//            if(kv._2.isInstanceOf[RDD[_]]){
//                kv._2.asInstanceOf[RDD[_]].unpersist(true)
//            }else if(kv._2.isInstanceOf[Dataset[_]]){
//                kv._2.asInstanceOf[Dataset[_]].unpersist(true)
//            }
//        })
      }
    })
  }
  def define(): DatasourceDefine = {
    val paramDefines = Map[String, DatasourceParamDefine](
      "kafka.bootstrapServers" -> new DatasourceParamDefine("kafka.bootstrap.servers", "127.0.0.1:9092"),
      "kafka.group" -> new DatasourceParamDefine("kafka.group.name", "group"),
      "kafka.topics" -> new DatasourceParamDefine("kafka.topic", "topic"),
      "kafka.duration" -> new DatasourceParamDefine("kafka.duration", "300"),
      "kafka.ds.name" -> new DatasourceParamDefine("kafka.ds.name", "DataStreamName"))
      
    val datasourceDefine = new DatasourceDefine("ds.kafka", paramDefines.toMap)
    datasourceDefine.id = "kafka_source_define"
    return datasourceDefine
  }
}