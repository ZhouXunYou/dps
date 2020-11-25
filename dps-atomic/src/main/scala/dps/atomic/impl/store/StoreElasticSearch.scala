package dps.atomic.impl.store

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.mutable.Map

/**
 * @author niewanxia
 * @date 11/25/20 2:36 PM
 **/
class StoreElasticSearch(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  override def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    val resource = params.get("resource").get
    EsSparkSQL.saveToEs(dataset, resource)
  }
  override def define: AtomOperationDefine = {
    val params = Map(
      "resource" -> new AtomOperationParamDefine("es.resource", "index/type", true, stringType))
    val atomOperation = new AtomOperationDefine(getClassName, getClassSimpleName, s"store/${getClassSimpleName}.ftl", params.toMap, classOf[Dataset[_]], classOf[Nothing], classOf[Row], classOf[Nothing])
    atomOperation.id = "store_elasticsearch.ftl"
    return atomOperation
  }
}
