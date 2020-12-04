package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    def doIt(params: Map[String, String]): Any = {
        val variable = variables.get(params.get("variableKey").get)
        if (variable.isInstanceOf[RDD[_]]) {
            variable.asInstanceOf[RDD[_]].unpersist(true);
        } else if (variable.isInstanceOf[Dataset[_]]) {
            variable.asInstanceOf[Dataset[_]].unpersist(true);
        }
        variables.remove(params.get("variableKey").get)
    }
}