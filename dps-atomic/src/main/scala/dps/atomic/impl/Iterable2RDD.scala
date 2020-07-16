package dps.atomic.impl

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.define.AtomOperationDefine

class Iterable2RDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val topicRDD = sparkSession.sparkContext.parallelize(this.pendingData.asInstanceOf[Iterable[Tuple3[String, Int, String]]].toSeq)
    val rdd = topicRDD.map(tuple=>{
      tuple._3
    })
    variables.put(outputVariableKey, rdd)
  }
  override def define: AtomOperationDefine = {
    val params = Map()
    val atomOperation = new AtomOperationDefine("Kafka Topic RDD", "iterable2RDD", "Iterable2RDD.flt", params.toMap)
    atomOperation.id = "iterable_to_rdd"
    return atomOperation
  }
}