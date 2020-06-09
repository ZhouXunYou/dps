package samples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import dps.datasource.DataSource
import dps.datasource.StreamDatasource
import dps.atomic.Operator

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Kafka_Receiver").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    import scala.collection.mutable.Map
    val map = Map[String, String]();
    map.put("duration", "5");
    
//    val operator = new Operator()
    
    val s2 = Class.forName("dps.datasource.KafkaSource")
      .getConstructor(classOf[SparkContext], classOf[Map[String, String]])
      .newInstance(sc, map)
      .asInstanceOf[DataSource]
    val rdd = s2.read("aaaa");
    rdd.asInstanceOf[RDD[String]].foreach(f=>{
      println(f)
    })
    s2.asInstanceOf[StreamDatasource].start()
  }
}