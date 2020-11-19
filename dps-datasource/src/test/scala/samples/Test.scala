package samples

import dps.atomic.Operator
import dps.atomic.model.OperationGroup
import dps.datasource.{DataSource, StreamDatasource}
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.List
import scala.collection.mutable.Map
import org.apache.spark.SparkConf

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
//    sparkConf.set(key, value)
    val map = Map[String, String]()
    map.put("duration", "10")
    map.put("bootstrapServers", "192.168.11.200:9092")
    map.put("group", "groupTest")
    map.put("topics", "logstash_test")
    
    sparkConf.setAppName("Kafka_Receiver")
    sparkConf.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val operationGroups: List[OperationGroup] = null
    val missionVariables: Map[String, Any] = Map[String, Any]()
    val o = new Operator(operationGroups, sparkSession, sparkConf,missionVariables)
    val s2 = Class.forName("dps.datasource.KafkaSource")
      .getConstructor(classOf[SparkSession], classOf[Map[String, String]], classOf[Operator])
      .newInstance(sparkSession, sparkConf, map, o)
      .asInstanceOf[DataSource]
    val rdd = s2.read();
    s2.asInstanceOf[StreamDatasource].start()
  }

}