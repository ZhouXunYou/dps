package ${packagePath}

import com.vividsolutions.jts.geom.Geometry
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

import scala.collection.mutable.Map

class ${className}(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
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
}