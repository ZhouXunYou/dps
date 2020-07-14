package dps.mission

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
    val sparkSession = builder.appName("kt").master("local[*]").getOrCreate();
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.11.207:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "aaa",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean))
    val topics = "userpath".split(",")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    stream.foreachRDD(records => {
      records.foreach(record=>{
        println(record.topic(),record.partition(),record.value());
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}