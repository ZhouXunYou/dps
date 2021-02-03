package ${packagePath}

import dps.atomic.define.{AtomOperationDefine, AtomOperationHasUdfDefine, AtomOperationParamDefine, AtomOperationUdf}
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.Map

class SpatialSQL(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val dataFrame = this.pendingData.asInstanceOf[DataFrame]
        val geoViewName = params.get("geoViewName").get
        val spatialSQL = params.get("spatialSQL").get
        val result = sparkSession.sql(spatialSQL)
        result.createOrReplaceTempView(geoViewName)
        variables.put(outputVariableKey, result)
    }
}