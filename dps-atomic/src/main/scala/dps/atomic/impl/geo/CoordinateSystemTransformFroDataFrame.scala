package dps.atomic.impl.geo

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.Map

class CoordinateSystemTransformFroDataFrame(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  override def doIt(params: Map[String, String]): Any = {
    val dataFrame = this.pendingData.asInstanceOf[DataFrame]
    val geoField = params.get("geoField").get
    val geoViewName = params.get("geoViewName").get
    val fromEpsgCRSCode = params.get("fromEpsgCRSCode").get
    val targetEpsgCRSCode = params.get("targetEpsgCRSCode").get
    val useLongitudeLatitudeOrder = params.get("targetEpsgCRSCode").getOrElse("false")
    val isStrict = params.get("strict").get.toBoolean
    sparkSession.sql(s"select ST_Transform(${geoField},${fromEpsgCRSCode},${targetEpsgCRSCode},${useLongitudeLatitudeOrder},${isStrict}),* from ${geoViewName}")
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "geoField" -> new AtomOperationParamDefine("geo.field", "Field", true, stringType),
      "geoViewName" -> new AtomOperationParamDefine("geo.view.name", "View Name", true, stringType),
      "fromEpsgCRSCode" -> new AtomOperationParamDefine("from.epsgCRS.code", "From EpsgCRS Code", true, stringType),
      "targetEpsgCRSCode" -> new AtomOperationParamDefine("target.epsgCRS.code", "From EpsgCRS Code", true, stringType),
      "useLongitudeLatitudeOrder" -> new AtomOperationParamDefine("use.longitude.latitude.order", "Use Longitude Latitude Order", false, stringType)
    )

    val template = s"geo/${getClassSimpleName}.ftl"
    val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[DataFrame], classOf[Nothing], classOf[Nothing], classOf[Nothing], getTemplateContent(template))
    return atomOperation
  }
}