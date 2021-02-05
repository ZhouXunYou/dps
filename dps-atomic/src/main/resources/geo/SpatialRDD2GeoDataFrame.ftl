package ${packagePath}

import com.vividsolutions.jts.geom.Geometry
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.Adapter

import scala.collection.mutable.Map


class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

    override def doIt(params: Map[String, String]): Any = {
        val spatialRDD = this.pendingData.asInstanceOf[SpatialRDD[Geometry]]
	    val fieldNames = params.get("fieldNames").get.split(",").toList
	    val dataFrame = Adapter.toDf(spatialRDD, fieldNames, sparkSession)
	    val geoViewName = params.get("geoViewName").get
	    dataFrame.createOrReplaceTempView(geoViewName)
	    val spatialFieldOriginalName = params.get("spatialFieldOriginalName").get
	    val spatialFieldAliasName = params.get("spatialFieldAliasName").getOrElse(spatialFieldOriginalName)
	    val names = ${fieldNames}
	    val geomDataFrame = sparkSession.sql(s"select ST_GeomFromWKT(${spatialFieldOriginalName}) as ${spatialFieldAliasName},<#noparse>${names.mkString(",")}</#noparse> from ${geoViewName}")
	    geomDataFrame.createOrReplaceTempView(geoViewName)
	    variables.put(outputVariableKey, geomDataFrame)
    }
}