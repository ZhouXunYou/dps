package dps.atomic.impl.store

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class StoreDatabaseUseJdbc(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        if (dataset != null && dataset.isEmpty) {
            return
        }
        val om = new ObjectMapper with ScalaObjectMapper
        val dsParams = om.readValue(sparkConf.get(params.get("dsParamsKey").get), classOf[Map[String, String]])
        val saveMode = SaveMode.valueOf(dsParams.getOrElse("saveMode", "Append"))
        dataset.write.format("jdbc")
            .option("driver", dsParams.get("driver").get)
            .option("url", dsParams.get("url").get)
            .option("dbtable", params.get("table").get)
            .option("user", dsParams.get("user").get)
            .option("password", dsParams.get("password").get).mode(saveMode).save()
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "dsParamsKey" -> new AtomOperationParamDefine("ds.params.key", "dsKey", true, mapType),
            "table" -> new AtomOperationParamDefine("table.name", "Tablename", true, stringType),
            "saveMode" -> new AtomOperationParamDefine("save.mode", ",Append,Overwrite", false, listType))
        val template = s"store/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[Dataset[_]], classOf[Nothing], classOf[Row], classOf[Nothing],getTemplateContent(template))
        return atomOperation
    }
}