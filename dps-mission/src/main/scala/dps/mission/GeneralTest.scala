package dps.mission

import java.io.Reader
import java.io.StringReader

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.geotools.geojson.geom.GeometryJSON
import org.locationtech.jts.geom.Geometry

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import org.neo4j.spark.Neo4j

object GeneralTest {
  def main(args: Array[String]): Unit = {

    val params: Map[String, String] = Map(
      "url" -> "jdbc:postgresql://192.168.11.201:5432/emmc",
      "driver" -> "org.postgresql.Driver",
      "user" -> "postgres",
      "password" -> "postgres")

    val builder = SparkSession.builder()
    val sparkConf = new SparkConf
    sparkConf.setAppName("test").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", "8g")
      .set("spark.neo4j.url", "bolt://192.168.11.201:7687")
      .set("spark.neo4j.user", "neo4j")
      .set("spark.neo4j.password", "a123456")
    builder.config(sparkConf)
    val sparkSession = builder.getOrCreate()

    val neo4j = new Neo4j(sparkSession.sparkContext)

    println("计算开始:" + System.currentTimeMillis())
    val new_neo4j: Neo4j = neo4j.cypher("match (n:BASE_STATION_LOGIC) return n.sys_moid as moid,n.attr_Region as area_id,toFloat(n.attr_Latitude) as latitude,toFloat(n.attr_Longitude) as longitude", params)
    val dataFrame: DataFrame = new_neo4j.loadDataFrame

    val security_areas = this.jdbcQuery(params, "(select id,name,region,area from t_security_area) as tmpView", sparkSession).rdd.collect()

    //    val path = "hdfs://cdhnode209:8020/emmc/humanmigrated"
    //    val df = sparkSession.sqlContext.read.load(path).select("logic_site_id", "latitude", "longitude").rdd

    val map = dataFrame.rdd.map(f => {
      val logicId = f.getAs("moid").asInstanceOf[String]
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
        "logicId" -> logicId,
        "securityIds" -> securityIds)
    })
    
    
    map.foreach(f => {
      println(f)
    })
    println(map.count())
    println("计算结束:" + System.currentTimeMillis())
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
}