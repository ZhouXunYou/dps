package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    def doIt(params: Map[String, String]): Any = {
        sparkSession.sqlContext.sql(s"msck repair table ${params.get("warehouse").get}.${params.get("table").get}");
    }
}