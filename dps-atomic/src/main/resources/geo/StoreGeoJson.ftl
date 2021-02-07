package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import com.vividsolutions.jts.geom.Geometry

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction
import java.io.File

class ${className}(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val spatialRDD = this.pendingData.asInstanceOf[SpatialRDD[Geometry]]
        spatialRDD.saveAsGeoJSON(params.get("outputPath").get.+(File.pathSeparator).+(System.currentTimeMillis()))
    }
}