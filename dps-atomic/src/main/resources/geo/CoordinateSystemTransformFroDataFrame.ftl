package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  override def doIt(params: Map[String, String]): Any = {
    val dataFrame = this.pendingData.asInstanceOf[DataFrame]
    val geoField = params.get("geoField").get
    val geoViewName = params.get("geoViewName").get
    val fromEpsgCRSCode = params.get("fromEpsgCRSCode").get
    val targetEpsgCRSCode = params.get("targetEpsgCRSCode").get
    val useLongitudeLatitudeOrder = params.get("useLongitudeLatitudeOrder").getOrElse("false")
    val isStrict = params.get("strictCheck").get.toBoolean
    sparkSession.sql(s"select ST_Transform(<#noparse>${geoField}</#noparse>,<#noparse>${fromEpsgCRSCode}</#noparse>,<#noparse>${targetEpsgCRSCode}</#noparse>,<#noparse>${useLongitudeLatitudeOrder}</#noparse>,<#noparse>${isStrict}</#noparse>),* from <#noparse>${geoViewName}</#noparse>")
  }
}