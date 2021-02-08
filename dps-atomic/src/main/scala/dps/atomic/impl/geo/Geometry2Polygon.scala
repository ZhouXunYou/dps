package dps.atomic.impl.geo

import scala.collection.mutable.Map

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.PolygonRDD
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

import dps.atomic.define.AtomOperationDefine
import dps.atomic.impl.AbstractAction

class Geometry2Polygon(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val polygonSpatialRDD = ShapefileReader.geometryToPolygon(this.pendingData.asInstanceOf[SpatialRDD[Geometry]])
        variables.put(outputVariableKey, polygonSpatialRDD)
    }

    override def define(): AtomOperationDefine = {
        val params = Map()
        val template = s"geo/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[SpatialRDD[_]], classOf[PolygonRDD], classOf[Geometry], classOf[Nothing], getTemplateContent(template))
        atomOperation
    }
}