package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._

import scala.collection.mutable.Map

class Neo4jQuery(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val url = params.get("url").get
    val user = params.get("user").get
    val password = params.get("password").get
    val sql = params.get("sql").get
    val viewName = params.get("viewName").get

//		val config = sparkSession.conf
//		var conf = new SparkConf().setAppName(config.get("spark.app.name")).setMaster(config.get("spark.master"))
//    conf.set("spark.neo4j.url", url)
//    conf.set("spark.neo4j.user", user)
//    conf.set("spark.neo4j.password", password)
//    val sc = new SparkContext(conf)
    import sparkSession.implicits._
    val neo4j = new Neo4j(sparkSession.sparkContext)
    val new_neo4j: Neo4j = neo4j.cypher(sql)

    val dataFrame: DataFrame = new_neo4j.loadDataFrame
    dataFrame.createOrReplaceTempView(viewName)
    dataFrame.show()
    this.variables.put(outputVariableKey, dataFrame)
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "url" -> new AtomOperationParamDefine("Neo4j URL", "bolt://127.0.0.1:7687", true, "1"),
      "user" -> new AtomOperationParamDefine("User", "neo4j", true, "1"),
      "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"),
      "sql" -> new AtomOperationParamDefine("Query SQL", "match(n:ci) return n limit 1", true, "3"),
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1"))
    val atomOperation = new AtomOperationDefine("Neo4j Query", "neo4jQuery", "Neo4jQuery.flt", params.toMap)
    atomOperation.id = "neo4j_query"
    return atomOperation
  }

}