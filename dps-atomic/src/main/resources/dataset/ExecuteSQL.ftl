package ${packagePath}

import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row,Dataset,SparkSession}

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    override def doIt(params: Map[String, String]): Any = {
        val sql = params.get("sql").get
        val viewName = params.get("viewName").get
        val dataset = sparkSession.sqlContext.sql(sql)
        dataset.createOrReplaceTempView(viewName)
        this.variables.put(outputVariableKey, dataset)
    }
}