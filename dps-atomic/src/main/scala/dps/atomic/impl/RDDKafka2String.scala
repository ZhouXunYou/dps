package dps.atomic.impl

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class RDDKafka2String(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val topicValue = this.pendingData.asInstanceOf[RDD[Tuple3[String, Int, String]]].filter(tuple=>{
      tuple._1.equals(params.get("tipicName").get)
    })
    val rdd = topicValue.map(tuple=>{
      tuple._3
    })
    variables.put(outputVariableKey, rdd)
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "tipicName" -> new AtomOperationParamDefine("Topic Name", "Topic Name", true, "1"))
    val atomOperation = new AtomOperationDefine("Kafka RDD Handle", "kafkaRddHandle", "RDDKafka2String.flt", params.toMap)
    atomOperation.id = "rdd_kafka_2_string"
    return atomOperation
  }
}