package dps.datasource

import scala.collection.mutable.Map

import org.apache.spark.sql.SparkSession

import dps.atomic.Operator
import dps.datasource.define.DatasourceDefine
import dps.datasource.define.DatasourceParamDefine
import org.apache.spark.SparkConf

class CommonSource{
//(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val params: Map[String, String], override val operator: Operator) extends DataSource(sparkSession, sparkConf, params, operator) {
//  def read(variableKey: String) {
//    operator.operation();
//  }
//  def define(): DatasourceDefine = {
//    val paramDefines = Map[String, DatasourceParamDefine](
//      "configParam" -> new DatasourceParamDefine("configParam", "{}"))
//    val datasourceDefine = new DatasourceDefine("Common", paramDefines.toMap)
//    datasourceDefine.id = "common_source_define"
//    return datasourceDefine
//  }
}