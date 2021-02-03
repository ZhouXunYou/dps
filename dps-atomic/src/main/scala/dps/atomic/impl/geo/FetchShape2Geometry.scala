package dps.atomic.impl.geo

import com.vividsolutions.jts.geom.Geometry
import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import dps.atomic.impl.AbstractAction

import scala.collection.mutable.Map
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.SpatialRDD

class FetchShape2Geometry(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val geometryRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, params.get("input").get)
        variables.put(outputVariableKey, geometryRDD)
    }

    override def define: AtomOperationDefine = {
        val params = Map()

        val template = s"geo/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[SpatialRDD[_]], classOf[Nothing], classOf[Geometry], classOf[Nothing], getTemplateContent(template))
        return atomOperation
    }
}