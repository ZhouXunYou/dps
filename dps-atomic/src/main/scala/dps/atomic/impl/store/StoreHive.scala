package dps.atomic.impl.store

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class StoreHive(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        val warehouse = params.get("warehouse").get
        val table = params.get("table").get
        val saveMode = SaveMode.valueOf(params.getOrElse("saveMode", "Append"))
        dataset.write.mode(SaveMode.Append).insertInto(s"${warehouse}.${table}")
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "warehouse" -> new AtomOperationParamDefine("warehouse.name", "default", true, stringType),
            "table" -> new AtomOperationParamDefine("table.name", "Table Name", true, stringType),
            "saveMode" -> new AtomOperationParamDefine("save.mode", ",Append,Overwrite", false, listType))
        val template = s"store/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[Dataset[_]], classOf[Nothing], classOf[Row], classOf[Nothing],getTemplateContent(template))
        return atomOperation
    }
}