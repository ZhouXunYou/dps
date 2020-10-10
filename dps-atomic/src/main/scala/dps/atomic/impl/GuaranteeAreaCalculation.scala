package dps.atomic.impl

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.io.StringReader
import org.geotools.geojson.geom.GeometryJSON
import org.locationtech.jts.geom.Geometry
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import java.io.Reader

class GuaranteeAreaCalculation(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val data = this.pendingData.asInstanceOf[Dataset[Row]].distinct().rdd

    val security_areas = this.jdbcQuery(params, "(select id,name,region,area from t_security_area) as tmpView", sparkSession).distinct().rdd.collect()

    data.map(f => {
      val logicId = f.getAs("id").asInstanceOf[String]
      val lng = f.getAs("longitude").asInstanceOf[Double]
      val lat = f.getAs("latitude").asInstanceOf[Double]
      val coordinate = new JSONArray()
      coordinate.add(lng)
      coordinate.add(lat)
      val geoPoint = new JSONObject
      geoPoint.put("type", "Point")
      geoPoint.put("coordinates", coordinate)
      val point: Geometry = new GeometryJSON().read(geoPoint.toString())
      var securityIds: String = new String

      for (i <- 0 to security_areas.length - 1) {
        val security = security_areas.apply(i)
        val securityId = security.getAs("id").asInstanceOf[String]
        val area = security.getAs("area").asInstanceOf[String]
        var reader: Reader = new StringReader(area)
        val poly: Geometry = new GeometryJSON().read(reader)
        if (poly.contains(point)) {
          if (securityIds.length() < 1) {
            securityIds += securityId
          } else {
            securityIds += ",".+(securityId)
          }
        }
      }
      Map(
        "id" -> logicId,
        "security_Id" -> securityIds)
    })
  }

  /**
   * JDBC查询返回Dataset[Row]
   *
   * @param params
   * @param sql
   * @return
   */
  private def jdbcQuery(params: Map[String, String], sql: String, sparkSession: SparkSession): Dataset[Row] = {
    sparkSession.sqlContext.read.format("jdbc")
      .option("url", params.get("url").get)
      .option("driver", params.get("driver").get)
      .option("dbtable", sql)
      .option("user", params.get("user").get)
      .option("password", params.get("password").get)
      .load();
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "driver" -> new AtomOperationParamDefine("JDBC Driver", "org.postgresql.Driver", true, "1"),
      "url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
      "sql" -> new AtomOperationParamDefine("Query SQL", "select * from dual", true, "3"),
      "user" -> new AtomOperationParamDefine("User", "user", true, "1"),
      "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"))
    val atomOperation = new AtomOperationDefine("Guarantee_Area_Calculation", "guaranteeAreaCalculation", "GuaranteeAreaCalculation.flt", params.toMap)
    atomOperation.id = "guarantee_area_calculation"
    return atomOperation
  }
}