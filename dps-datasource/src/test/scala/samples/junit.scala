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
    
    lines.print()
    
    ssc.start()
    ssc.awaitTermination()
  }

}
