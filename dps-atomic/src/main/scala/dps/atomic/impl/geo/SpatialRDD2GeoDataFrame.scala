package dps.atomic.impl.geo

import scala.collection.mutable.Map

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class SpatialRDD2GeoDataFrame(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val spatialRDD = this.pendingData.asInstanceOf[SpatialRDD[Geometry]]
        val fieldNames = params.get("fieldNames").get.split(",").toList
        val dataFrame = Adapter.toDf(spatialRDD, fieldNames, sparkSession)
        val geoViewName = params.get("geoViewName").get
        dataFrame.createOrReplaceTempView(geoViewName)
        variables.put(outputVariableKey, dataFrame)
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "fieldNames" -> new AtomOperationParamDefine("geo.field.names", "Field Names", true, stringType),
            "geoViewName" -> new AtomOperationParamDefine("geo.view.name", "Geo View Name", true, stringType),
            "spatialFieldOriginalName" -> new AtomOperationParamDefine("spatial.field.original.name", "Spatial Field Original Name", true, stringType),
            "spatialFieldAliasName" -> new AtomOperationParamDefine("spatial.field.alias.name", "Spatial Field Alias Name", false, stringType))

        val template = s"geo/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[SpatialRDD[_]], classOf[DataFrame], classOf[Geometry], classOf[Nothing], getTemplateContent(template))
        atomOperation
    }
}