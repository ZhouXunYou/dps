package ${packagePath}

import com.vividsolutions.jts.geom.Geometry
import dps.atomic.define.AtomOperationDefine
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.{LineStringRDD, SpatialRDD}

import scala.collection.mutable.Map

class Geometry2LineString(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val lineStringSpatialRDD = ShapefileReader.geometryToLineString(this.pendingData.asInstanceOf[SpatialRDD[Geometry]])
        variables.put(outputVariableKey, lineStringSpatialRDD)
    }
}