package dps.atomic.impl.rdd

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class MapRDD2TupleRDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[Map[String, Any]]]
        val primaryKey = params.get("primaryKey").get.toString()
        val tuple = rdd.map(map => {
            processMap(map, primaryKey)
        })
    }
    private def processMap(map: Map[String, Any], primaryKey: String): Tuple2[String, Map[String, Any]] = {
        return (map.get(primaryKey).get.toString(), map)
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "primaryKey" -> new AtomOperationParamDefine(
                "primary.key", "Primary Key", true, stringType))
        val template = s"rdd/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[RDD[_]], classOf[RDD[_]], classOf[String], classOf[Map[String, Any]],getTemplateContent(template))
        return atomOperation
    }
}