package dps.atomic.temp

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class TupleMapRDDJoin(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val leftRDD = params.get("left").get.asInstanceOf[RDD[Tuple2[String, Map[String,Any]]]]
    val rightRDD = params.get("right").get.asInstanceOf[RDD[Tuple2[String, Map[String,Any]]]]
    val joinRDD = leftRDD.join(rightRDD)
    val newMap = joinRDD.map(tuple=>{
        tuple._2._1++tuple._2._2
    })
    return newMap
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "left" -> new AtomOperationParamDefine("left.rdd", "Left RDD", true, stringType),
      "right" -> new AtomOperationParamDefine("right.rdd", "Right RDD", true, stringType)
    )
    val atomOperation = new AtomOperationDefine(getClassName, getClassSimpleName, s"rdd/${getClassSimpleName}.flt", params.toMap,classOf[RDD[_]],classOf[RDD[_]],classOf[Tuple2[String, Map[String,Any]]],classOf[Map[String,Any]])
    atomOperation.id = "rdd_join"
    return atomOperation
  }
}