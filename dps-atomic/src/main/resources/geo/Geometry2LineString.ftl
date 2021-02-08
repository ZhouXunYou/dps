package ${packagePath}

import scala.collection.mutable.Map

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.LineStringRDD
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry

import dps.atomic.define.AtomOperationDefine
import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val lineStringSpatialRDD = ShapefileReader.geometryToLineString(this.pendingData.asInstanceOf[SpatialRDD[Geometry]])
        variables.put(outputVariableKey, lineStringSpatialRDD)
    }
}