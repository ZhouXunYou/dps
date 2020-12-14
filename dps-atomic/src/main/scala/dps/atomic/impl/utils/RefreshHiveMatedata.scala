package dps.atomic.impl.utils

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class RefreshHiveMatedata(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

    def doIt(params: Map[String, String]): Any = {
        sparkSession.sqlContext.sql(s"msck repair table ${params.get("warehouse").get}.${params.get("table").get}");
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "warehouse" -> new AtomOperationParamDefine("warehouse.name", "default", true, stringType),
            "table" -> new AtomOperationParamDefine("table.name", "Table Name", true, stringType))
        val template = s"utils/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[Nothing], classOf[Nothing], classOf[Nothing], classOf[Nothing],getTemplateContent(template))
        return atomOperation
    }
}