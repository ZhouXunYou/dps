package dps.atomic.temp

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import dps.atomic.impl.AbstractAction

class RDDJoinRDD(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val leftRDD = params.get("leftVariableKey").get.asInstanceOf[RDD[Tuple2[String, String]]]
    val rightRDD = params.get("rightVariableKey").get.asInstanceOf[RDD[Tuple2[String, String]]]
    //e.g: left = (a:b,c,d),right = (a:e,f,g),result = (a:b,c,d,e,f,g)
    leftRDD.join(rightRDD)
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "leftVariableKey" -> new AtomOperationParamDefine("Left RDD", "Left RDD Variable Key", true, "1"),
      "rightVariableKey" -> new AtomOperationParamDefine("Right RDD", "Right RDD Variable Key", true, "1")
    )
    val atomOperation = new AtomOperationDefine("RDD Join", "rddJoin", "RDDJoinRDD.flt", params.toMap,classOf[Nothing],classOf[Nothing],classOf[Nothing],classOf[Nothing])
    atomOperation.id = "rdd_join"
    return atomOperation
  }
}