package samples

import org.junit._
import Assert._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds,StreamingContext,kafka010}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import dps.datasource.StreamDatasource
import dps.datasource.DataSource

@Test
class AppTest {

//  @Test
//  def testOK() = assertTrue(true)

  //    @Test
  //    def testKO() = assertTrue(false)

  @Test
  def kafka() {
    val sparkConf: SparkConf = new SparkConf().setAppName("Kafka_Receiver").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(5))
    
    val kafkaParams=Map[String,Object](
      "bootstrap.servers"->"192.168.36.244:9092",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->"groupName",
      "auto.offset.reset"->"latest",
      "enable.auto.commit"->(true:java.lang.Boolean)
      )
    val topics=List("DATAPACKAGE_QUEUE")
    val lines=KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )
    
    lines.foreachRDD(rdd=>{
      rdd.map(record=>{
        val array = record.value().split(",")
        Map[String,String](array.apply(0)->array.apply(1))
      }).reduce((a,b)=>{
        b
      })
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
  @Test
  def typeTest() {
    val sparkConf: SparkConf = new SparkConf().setAppName("Kafka_Receiver").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    import scala.collection.mutable.Map
    val map = Map[String, String]();
    map.put("duration", "555555");
    val s1 = Class.forName("dps.datasource.FileSource")
      .getConstructor(classOf[SparkContext], classOf[Map[String, String]])
      .newInstance(sc, map)
      .asInstanceOf[DataSource]
    
    val s2 = Class.forName("dps.datasource.KafkaSource")
      .getConstructor(classOf[SparkContext], classOf[Map[String, String]])
      .newInstance(sc, map)
      .asInstanceOf[DataSource]
    
    println(s1.isInstanceOf[DataSource])
    println(s2.isInstanceOf[StreamDatasource])
   
  }
  def numTest(){
    
  }
}
