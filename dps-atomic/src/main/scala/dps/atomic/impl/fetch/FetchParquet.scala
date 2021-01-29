package dps.atomic.impl.fetch

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row,Dataset}
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class FetchParquet(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

    override def doIt(params: Map[String, String]): Any = {
        val path = params.get("path").get
        val viewName = params.get("viewName").get
        val dataset = sparkSession.sqlContext.read.load(path);
        dataset.createOrReplaceTempView(viewName);
        this.variables.put(outputVariableKey, dataset);
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "path" -> new AtomOperationParamDefine("parquet.path", "local or distributed file system path", true, stringType),
            "viewName" -> new AtomOperationParamDefine("abstract.view.name", "View Name", true, stringType))
        val template = s"fetch/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[Nothing], classOf[Dataset[_]], classOf[Nothing], classOf[Row],getTemplateContent(template))
        return atomOperation
    }
}