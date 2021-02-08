package dps.atomic.impl.geo

import scala.collection.mutable.Map

import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction
import org.apache.sedona.core.spatialRDD.SpatialRDD


class GeoDataFrame2SpatialRDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val geoDataFrame = this.pendingData.asInstanceOf[DataFrame]
        val spatialFieldName = params.get("spatialFieldName").get
        val fieldNames = params.get("fieldNames").get.split(",").toList
        val spatialRDD = Adapter.toSpatialRdd(geoDataFrame, spatialFieldName, fieldNames)
        variables.put(outputVariableKey, spatialRDD)
    }

    override def define(): AtomOperationDefine = {
        val params = Map(
            "spatialFieldName" -> new AtomOperationParamDefine("spatial.field.name", "Spatial Field Name", true, stringType),
            "fieldNames" -> new AtomOperationParamDefine("field.names", "Field Names", true, stringType)
        )
        val template = s"geo/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[DataFrame], classOf[SpatialRDD[_]], classOf[Nothing], classOf[Geometry], getTemplateContent(template))
        atomOperation
    }
}