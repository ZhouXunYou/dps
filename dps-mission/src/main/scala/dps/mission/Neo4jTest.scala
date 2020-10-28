package dps.mission

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.neo4j.spark._

object Neo4jTest {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
    builder.appName("neo4jTest")
    val conf = new SparkConf
    conf.set("spark.master", "local[*]")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.set("spark.neo4j.url", "bolt://192.168.11.201:7687")
    conf.set("spark.neo4j.user", "neo4j")
    conf.set("spark.neo4j.password", "a123456")

    builder.config(conf)
    val sparkSession = builder.getOrCreate()

    val neo4j = new Neo4j(sparkSession.sparkContext)

    val array: Array[String] = Array("013a13f7d40c44aaa8fdcdc30536c39a", "8ee845f096e84395853f2387d75e6738")

    val params: Map[String, Any] = Map("moid" -> array)

    val new_neo4j: Neo4j = neo4j.cypher("match (n:BASE_STATION_LOGIC) where n.sys_moid in($moid) return n.sys_moid as moid,n.sys_type as type,n.attr_ItemName as name,n.attr_Region as area_id,toFloat(n.attr_Latitude) as latitude,toFloat(n.attr_Longitude) as longitude,n.attr_Org as org_id", params)
    val dataFrame: DataFrame = new_neo4j.loadDataFrame

    dataFrame.show()
    //    dataFrame.createOrReplaceTempView("bsl")
  }

}