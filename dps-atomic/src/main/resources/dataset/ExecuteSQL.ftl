package ${packagePath}

import dps.atomic.impl.AbstractAction
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import org.apache.spark.SparkConf

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    def doIt(params: Map[String, String]): Any = {
        val sql = params.get("sql").get
        val viewName = params.get("viewName").get
        val dataset = sparkSession.sqlContext.sql(sql)
        dataset.createOrReplaceTempView(viewName)
        this.variables.put(outputVariableKey, dataset)
    }
}