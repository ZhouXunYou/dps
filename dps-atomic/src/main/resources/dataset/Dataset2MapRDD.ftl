package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    override def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        dataset.rdd.map(row => {
            rowTransformMap(row)
        })
    }

    private def rowTransformMap(row: Row): Map[String, Any] = {
        ${row2MapCode}
    }
}