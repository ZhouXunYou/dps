package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    override def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        if (dataset != null && dataset.isEmpty) {
            return
        }
        val partitionNum = params.get("partitionNum").getOrElse(String.valueOf(sparkSession.sparkContext.defaultMinPartitions)).toInt
        val path = params.get("storePath").get
        val partitionFields = params.getOrElse("partitionFields", "")
        val saveMode: SaveMode = SaveMode.valueOf(params.get("saveMode").getOrElse("Append"))
        if (partitionFields != null && !"".equals(partitionFields)) {
            dataset.repartition(partitionNum).write.mode(saveMode).partitionBy(partitionFields.split(","): _*).parquet(path)
        } else {
            dataset.repartition(partitionNum).write.mode(saveMode).parquet(path)
        }
    }
}