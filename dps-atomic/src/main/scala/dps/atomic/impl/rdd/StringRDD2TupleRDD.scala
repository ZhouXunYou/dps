package dps.atomic.impl.rdd

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class StringRDD2TupleRDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[String]]
        val separator = params.get("separator").get.toString()
        val index = params.get("index").get.toInt
        val tuple = rdd.map(value => {
            processString(value, separator, index)
        })
    }
    def processString(value: String, separator: String, index: Int): Tuple2[String, String] = {
        (string2Array(value, separator).apply(index), value)
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "separator" -> new AtomOperationParamDefine("value.separator", "separator", true, stringType),
            "index" -> new AtomOperationParamDefine("split.index", "index", true, integerType))
        val template = s"rdd/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[RDD[_]], classOf[RDD[_]], classOf[String], classOf[Tuple2[String, String]],getTemplateContent(template))
        return atomOperation
    }
}