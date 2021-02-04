package ${packagePath}

import com.vividsolutions.jts.geom.Geometry
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter

import scala.collection.mutable.Map


class ${className}(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val geoDataFrame = this.pendingData.asInstanceOf[DataFrame]
        val spatialFieldName = params.get("spatialFieldName").get
        val fieldNames = params.get("fieldNames").get.split(",").toList
        val spatialRDD = Adapter.toSpatialRdd(geoDataFrame, spatialFieldName, fieldNames)
        variables.put(outputVariableKey, spatialRDD)
    }
}