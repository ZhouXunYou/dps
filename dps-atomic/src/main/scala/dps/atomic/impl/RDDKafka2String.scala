package dps.atomic.impl

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map

class RDDKafka2String(override val sparkSession: SparkSession, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends dps.atomic.impl.AbstractAction(sparkSession, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val kafkTuple = this.pendingData.asInstanceOf[RDD[Tuple3[String, Int, String]]]
    val groupTopic = kafkTuple.groupBy(tuple => tuple._1).collect()
    groupTopic.foreach(topic => {
      val topicName = topic._1
      val topicRDD = sparkSession.sparkContext.parallelize(topic._2.toSeq)
      val valueRDD = topicRDD.map(tuple => {
        tuple._3
      })
      variables.put(outputVariableKey + "_" + topicName, valueRDD)
    })
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1"))
    val atomOperation = new AtomOperationDefine("Kafka RDD Handle", "kafkaRddHandle", "RDDKafka2String.flt", params.toMap)
    atomOperation.id = "rdd_kafka_2_string"
    return atomOperation
  }
}