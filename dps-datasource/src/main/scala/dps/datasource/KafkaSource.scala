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

class KafkaSource(override val sparkContext: SparkContext, override val params: Map[String, String]) extends StreamDatasource(sparkContext, params) {
  override def read(): Any = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.36.244:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "groupName",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean))
    val topics = Array("DATAPACKAGE_QUEUE")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val rdds: RDD[String] = sparkContext.emptyRDD[String]
    stream.foreachRDD(rdd => {
      val lineRDD = rdd.map(r => {
        r.value()
      })
      rdds.union(lineRDD);
    })
    
    return rdds
  }
  def define(): DatasourceDefine = {
    null
  }
}