package dps.atomic.impl.rdd

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class TupleStringRDDJoin(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val leftRDD = params.get("left").get.asInstanceOf[RDD[Tuple2[String, String]]]
        val rightRDD = params.get("right").get.asInstanceOf[RDD[Tuple2[String, String]]]
        val joinRDD = leftRDD.join(rightRDD)
        val newRDD = joinRDD.map(tuple => {
            tuple._2._1 ++ tuple._2._2
        })
        return newRDD
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "left" -> new AtomOperationParamDefine("left.string.rdd", "Left String RDD", true, stringType),
            "right" -> new AtomOperationParamDefine("right.string.rdd", "Right String RDD", true, stringType))
        val template = s"rdd/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[RDD[_]], classOf[RDD[_]], classOf[Tuple2[String, String]], classOf[String],getTemplateContent(template))
        return atomOperation
    }
}