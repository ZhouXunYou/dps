package dps.atomic.impl.geo

import scala.collection.mutable.Map

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction
import org.apache.sedona.core.spatialRDD.SpatialRDD

class FetchShape2Geometry(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val geometryRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, params.get("shpInput").get)
        variables.put(outputVariableKey, geometryRDD)
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "shpInput" -> new AtomOperationParamDefine("shp.file.input", "Shape File Input", true, stringType))
        val template = s"geo/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[SpatialRDD[_]], classOf[Nothing], classOf[Geometry], classOf[Nothing], getTemplateContent(template))
        return atomOperation
    }
}