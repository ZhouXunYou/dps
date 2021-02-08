package dps.atomic.impl.geo

import scala.collection.mutable.Map

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Geometry

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class CoordinateSystemTransformFroSpatialRDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  override def doIt(params: Map[String, String]): Any = {
    val spatialRDD = this.pendingData.asInstanceOf[SpatialRDD[Geometry]]
    val sourceCRS = CRS.decode(params.get("fromEpsgCRSCode").get)
    val targetCRS = CRS.decode(params.get("targetEpsgCRSCode").get)
    val isStrict = params.get("strict").get.toBoolean
    val transform = CRS.findMathTransform(sourceCRS, targetCRS, isStrict)
    val rdd = spatialRDD.rawSpatialRDD.rdd.map(geom => {
      JTS.transform(geom, transform)
    })
    spatialRDD.rawSpatialRDD = rdd.toJavaRDD()
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "fromEpsgCRSCode" -> new AtomOperationParamDefine("from.epsgCRS.code", "From EpsgCRS Code", true, stringType),
      "targetEpsgCRSCode" -> new AtomOperationParamDefine("target.epsgCRS.code", "From EpsgCRS Code", true, stringType),
      "isStrict" -> new AtomOperationParamDefine("is.strict", "Is Strict", true, stringType)
    )

    val template = s"geo/${getClassSimpleName}.ftl"
    val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[SpatialRDD[_]], classOf[Nothing], classOf[Geometry], classOf[Nothing], getTemplateContent(template))
    atomOperation
  }
}