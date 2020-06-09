package samples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import dps.datasource.DataSource
import dps.datasource.StreamDatasource
import dps.atomic.Operator
import dps.atomic.model.OperationGroup
import scala.collection.mutable.Map
import scala.collection.immutable.List

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Kafka_Receiver").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    import scala.collection.mutable.Map
    val map = Map[String, String]();
    map.put("duration", "10");
    
    val operationGroups:List[OperationGroup]=null
    val missionVariables:Map[String, Any]=Map[String,Any]()
    val o = new Operator(operationGroups,sc,missionVariables)
    val s2 = Class.forName("dps.datasource.KafkaSource")
      .getConstructor(classOf[SparkContext], classOf[Map[String, String]], classOf[Operator])
      .newInstance(sc, map,o)
      .asInstanceOf[DataSource]
    val rdd = s2.read("a");
//    rdd.asInstanceOf[RDD[String]].foreach(f=>{
//      println(f)
//    })
    s2.asInstanceOf[StreamDatasource].start()
  }
}