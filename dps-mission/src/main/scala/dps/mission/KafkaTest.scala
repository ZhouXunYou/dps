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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import java.util.Properties
import dps.atomic.util.KafkaSink
import scala.collection.mutable.Map
import org.apache.spark.broadcast.Broadcast
import dps.atomic.util.KafkaSink
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.neo4j.spark.Neo4j
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.Dataset

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
    
    val builder = SparkSession.builder()
    builder.appName("neo4jTest")
    val conf = new SparkConf
    conf.set("spark.master", "local[*]")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.driver.allowMultipleContexts", "true")
    
    conf.set("spark.neo4j.url", "bolt://192.168.11.201:7687")
    conf.set("spark.neo4j.user", "neo4j")
    conf.set("spark.neo4j.password", "a123456")

    builder.config(conf)
    val sparkSession = builder.getOrCreate()
    
    val params = Map(
          "bootstrapServers" -> "192.168.11.201:9092",
          "sendTopicName" -> "KAFKA_SEND_my1024"
    )
    
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", params.get("bootstrapServers").get)
        properties.setProperty("key.serializer", classOf[StringSerializer].getName)
        properties.setProperty("value.serializer", classOf[StringSerializer].getName)
        properties
      }
      import sparkSession.implicits
      val context = sparkSession.sparkContext
      context.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    val sendTopicName = params.get("sendTopicName").get
    
    val neo4j = new Neo4j(sparkSession.sparkContext)

    val new_neo4j: Neo4j = neo4j.cypher("match (n:BASE_STATION_LOGIC) return n.sys_moid as moid,n.sys_type as type,n.attr_ItemName as name,n.attr_Region as area_id,toFloat(n.attr_Latitude) as latitude,toFloat(n.attr_Longitude) as longitude,n.attr_Org as org_id limit 10")
    val dataFrame: DataFrame = new_neo4j.loadDataFrame
    
    dataFrame.toJSON.rdd.foreach(value=>{
      println(value)
      kafkaProducer.value.send(sendTopicName, value)
    })
  }
}