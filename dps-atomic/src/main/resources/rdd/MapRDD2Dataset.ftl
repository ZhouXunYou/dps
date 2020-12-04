package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row,Dataset}
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[Map[String, Any]]]
        val viewName = params.get("viewName").get
        val result = rdd.map(map => {
            buildRow(map)
        })
        var schema = StructType(specifyTableFields())
        val dataframe = sparkSession.sqlContext.createDataFrame(result, schema)
        dataframe.createOrReplaceTempView(viewName)
        this.variables.put(outputVariableKey, dataframe);
    }

    private def specifyTableFields(): List[StructField] = {
        ${buildTableFieldCode}
    }

    private def buildRow(map: Map[String, Any]): Row = {
        ${buildTableRowCode}
    }
}