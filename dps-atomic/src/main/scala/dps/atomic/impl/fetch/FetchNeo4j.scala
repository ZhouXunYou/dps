package dps.atomic.impl.fetch

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row,Dataset}
import org.apache.spark.sql.SparkSession
import org.neo4j.spark.Neo4j

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class FetchNeo4j(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val sql = params.get("sql").get
        val viewName = params.get("viewName").get
        val neo4j = new Neo4j(sparkSession.sparkContext)
        val new_neo4j: Neo4j = neo4j.cypher(sql)
        val dataset = new_neo4j.loadDataFrame
        dataset.createOrReplaceTempView(viewName)
        this.variables.put(outputVariableKey, dataset)
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "graphql" -> new AtomOperationParamDefine("graphql", "match(n:ci) return n limit 1", true, sqlType),
            "viewName" -> new AtomOperationParamDefine("abstract.view.name", "View Name", true, stringType))
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, s"fetch/${getClassSimpleName}.ftl", params.toMap, classOf[Nothing], classOf[Dataset[_]], classOf[Nothing], classOf[Row])
        return atomOperation
    }
}