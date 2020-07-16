package dps.mission

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.kafka010.PreferConsistent
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.Duration
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import scala.collection.mutable.Map
import dps.utils.JsonUtils
import org.apache.commons.collections.list.AbstractLinkedList.LinkedListIterator
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.{ StructField, StructType }

object KafkaTest {
  def main(args: Array[String]): Unit = {
//    val builder = SparkSession.builder()
//    val sparkConf = new SparkConf
//    sparkConf.setAppName("a").setMaster("local[*]")
//    builder.config(sparkConf)
//    val sparkSession = builder.enableHiveSupport().getOrCreate()
//    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(("10").toLong))
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "192.168.11.207:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "kkkkkk",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (true: java.lang.Boolean))
//    val topics = "usertest".split(",")
//    val stream = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams))
//    
//    var map = Map[String, Any]()
//    map.put("expression", "usertest")
//    
//    
//    stream.foreachRDD(records => {
//      if(!records.isEmpty()){
//        val topics = records.map(topic => {
//          (topic.topic(), topic.partition(), topic.value())
//        })
//        map.put("aaaaa", topics)
//        val topicRDD = topics.filter(tuple=>{
//          tuple._1.equals(map.get("expression").get)
//        })
//        topicRDD.foreach(aaa=>{
//          println(aaa._3)
//        })
////        map.put("aaaaa", topics)
////        println(map)
//      }
//    })
//    
//    streamingContext.start()
//    streamingContext.awaitTermination()
  }
}