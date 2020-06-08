package samples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import dps.datasource.DataSource
import dps.datasource.StreamDatasource

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Kafka_Receiver").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    import scala.collection.mutable.Map
    val map = Map[String, String]();
    map.put("duration", "5");
//    val s1 = Class.forName("dps.datasource.FileSource")
//      .getConstructor(classOf[SparkContext], classOf[Map[String, String]])
//      .newInstance(sc, map)
//      .asInstanceOf[DataSource]
    
    val s2 = Class.forName("dps.datasource.KafkaSource")
      .getConstructor(classOf[SparkContext], classOf[Map[String, String]])
      .newInstance(sc, map)
      .asInstanceOf[DataSource]
    val rdd = s2.read();
    rdd.asInstanceOf[RDD[String]].foreach(f=>{
      println(f)
    })
    s2.asInstanceOf[StreamDatasource].start()
    
//    println(s1.isInstanceOf[StreamDatasource])
//    println(s2.isInstanceOf[StreamDatasource])
  }
}