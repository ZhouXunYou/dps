package dps.atomic.temp

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.{ SparkConf }
import org.neo4j.spark._
import scala.collection.mutable.Map
import dps.atomic.impl.AbstractAction

class Neo4jQuery(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

    def doIt(params: Map[String, String]): Any = {

        val sql = params.get("sql").get
        val viewName = params.get("viewName").get

        import sparkSession.implicits._
        val neo4j = new Neo4j(sparkSession.sparkContext)
        val new_neo4j: Neo4j = neo4j.cypher(sql)

        val dataFrame: DataFrame = new_neo4j.loadDataFrame
        dataFrame.createOrReplaceTempView(viewName)
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