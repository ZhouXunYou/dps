package dps.mission

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._

object Neo4jTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("neo4jTest").setMaster("local[*]")
    conf.set("spark.neo4j.url", "bolt://192.168.11.200:7687")
    conf.set("spark.neo4j.user", "neo4j")
    conf.set("spark.neo4j.password", "a123456")
    val sc = new SparkContext(conf)

    val neo4j = new Neo4j(sc)

    val new_neo4j: Neo4j = neo4j.cypher("match(n:PERSONNEL) return n.sys_moid")
    val dataFrame: DataFrame = new_neo4j.loadDataFrame
    
    dataFrame.show()

  }
}