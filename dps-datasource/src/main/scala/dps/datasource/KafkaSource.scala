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

class KafkaSource(override val sparkContext: SparkContext, override val params: Map[String, String],override val operator:Operator) extends StreamDatasource(sparkContext, params,operator) {
  override def read(variableKey:String) {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.11.200:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "groupName",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean))
    val topics = Array("logstash_test")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    var rdds: RDD[String] = sparkContext.emptyRDD[String]
    
    stream.foreachRDD(records => {
      records.foreachPartition(recordPartition=>{
        recordPartition.foreach(record=>{
          //TODO 设置任务变量
//          operator.setVariable(variableKey, record)
//          operator.
        })
      })
      operator.operation()
//      val lineRDD = rdd.map(r => {
//        r.value()
//      })
//      lineRDD.foreach(str=>{
//        println(str)
//      })
//      rdds.union(lineRDD)
//      rdds.foreach(l=>{
//        println(l)
//      })
    })
    
    return rdds
  }
  def define(): DatasourceDefine = {
    null
  }
}