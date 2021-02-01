package dps.atomic.impl.geo
import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import org.apache.spark.sql.Dataset
import dps.atomic.define.AtomOperationUdf
import dps.atomic.define.AtomOperationHasUdfDefine
import org.apache.spark.sql.Row
import java.util.concurrent.TimeUnit

class SpatialSQL(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val dataFrame = this.pendingData.asInstanceOf[DataFrame]
        val geoViewName = params.get("geoViewName").get
        val spatialSQL = params.get("spatialSQL").get
        val result = sparkSession.sql(spatialSQL)
        result.createOrReplaceTempView(geoViewName)
        variables.put(outputVariableKey, result)
        
    }
    override def define(): AtomOperationDefine = {
        val params = Map(
            "geoViewName" -> new AtomOperationParamDefine("geo.view.name", "Geometry View Name", true, stringType), //JDBC
            "spatialSQL" -> new AtomOperationParamDefine("spatialSQL", "select * from dual", true, sqlType))
        val udfs = Seq(
            new AtomOperationUdf("ST_Distance", Seq(classOf[String].getName,classOf[String].getName)),
            new AtomOperationUdf("ST_ConvexHull", Seq(classOf[String].getName)),
            new AtomOperationUdf("ST_Envelope", Seq(classOf[String].getName)))
        val template = s"geo/${getClassSimpleName}.ftl"
        val atomOperation = new AtomOperationHasUdfDefine(getId, getClassName, getClassSimpleName, template, params.toMap, classOf[Nothing], classOf[Dataset[_]], classOf[Nothing], classOf[Row], getTemplateContent(template),udfs)
        return atomOperation
    }
    
}