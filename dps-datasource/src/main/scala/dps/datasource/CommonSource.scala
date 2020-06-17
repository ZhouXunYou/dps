package dps.datasource

import scala.collection.mutable.Map

import org.apache.spark.sql.SparkSession

import dps.atomic.Operator
import dps.datasource.define.DatasourceDefine
import dps.datasource.define.DatasourceParamDefine

class CommonSource (override val sparkSession: SparkSession,override val params: Map[String, String],override val operator:Operator) extends DataSource(sparkSession, params,operator){
  def read(variableKey:String) {
    operator.operation();
  }
  def define(): DatasourceDefine = {
    val paramDefines = Map[String, DatasourceParamDefine](
      "configParam" -> new DatasourceParamDefine("configParam", "{}")
      )
    val datasourceDefine = new DatasourceDefine("Common", paramDefines.toMap)
    datasourceDefine.id = "common_source_define"
    return datasourceDefine
  }
}