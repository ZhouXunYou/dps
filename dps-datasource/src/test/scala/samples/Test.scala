package samples

import java.util.Optional
import java.util.function.ToLongFunction
import dps.datasource.StreamDatasource
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import dps.datasource.DataSource

object Test {
  def main(args: Array[String]): Unit = {
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
    
    println(s1.isInstanceOf[StreamDatasource])
    println(s2.isInstanceOf[StreamDatasource])
  }
}