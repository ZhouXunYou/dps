package dps.atomic.impl.utils

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class ReleaseMemoryResource(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val variable = variables.remove(params.get("variableKey").get).get
        if (variable.isInstanceOf[RDD[_]]) {
            variable.asInstanceOf[RDD[_]].unpersist(true);
        } else if (variable.isInstanceOf[Dataset[_]]) {
            variable.asInstanceOf[Dataset[_]].unpersist(true);
        }
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "variableKey" -> new AtomOperationParamDefine("variable.key", "Variable Name", true, stringType))
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, s"utils/${getClassSimpleName}.ftl", params.toMap, classOf[Nothing], classOf[Nothing], classOf[Nothing], classOf[Nothing])
        return atomOperation
    }
}