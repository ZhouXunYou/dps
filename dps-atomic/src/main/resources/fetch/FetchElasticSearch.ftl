package ${packagePath}
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable.Map

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

    override def doIt(params: Map[String, String]): Any = {
        val viewName = params.get("viewName").get
        val query = params.get("query").get
        // resources es中的索引库和类型格式为：index/type
        val resource = params.get("resource").get
        val dataFrame = EsSparkSQL.esDF(sparkSession, resource, query)
        dataFrame.createOrReplaceTempView(viewName)
        this.variables.put(outputVariableKey, dataFrame)
    }
}