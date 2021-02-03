package dps.atomic.impl.geo

import com.vividsolutions.jts.geom.Geometry
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter

import scala.collection.mutable.Map

class SpatialRDD2GeoDataFrame(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  override def doIt(params: Map[String, String]): Any = {
    val spatialRDD = this.pendingData.asInstanceOf[SpatialRDD[Geometry]]
    val fieldNames = params.get("fieldNames").get.split(",").toList
    val dataFrame = Adapter.toDf(spatialRDD, fieldNames, sparkSession)
    val geoViewName = params.get("geoViewName").get
    dataFrame.createOrReplaceTempView(geoViewName)
    val spatialFieldOriginalName = params.get("spatialFieldOriginalName").get
    val spatialFieldAliasName = params.get("spatialFieldAliasName").getOrElse(spatialFieldOriginalName)
    val geomDataFrame = sparkSession.sql(s"select ST_GeomFromWKT(${spatialFieldOriginalName}) as ${spatialFieldAliasName},${fieldNames.mkString(",")} from ${geoViewName}")
    geomDataFrame.createOrReplaceTempView(geoViewName)
    variables.put(outputVariableKey, geomDataFrame)
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "fieldNames" -> new AtomOperationParamDefine("geo.field.names", "Field Names", true, stringType),
      "geoViewName" -> new AtomOperationParamDefine("geo.view.name", "Geo View Name", true, stringType),
      "spatialFieldOriginalName" -> new AtomOperationParamDefine("spatial.field.original.name", "Spatial Field Original Name", true, stringType))

    val template = s"rdd/${getClassSimpleName}.ftl"
    val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[SpatialRDD[_]], classOf[DataFrame], classOf[Geometry], classOf[Nothing], getTemplateContent(template))
    atomOperation
  }
}