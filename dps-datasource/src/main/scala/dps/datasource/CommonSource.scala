package dps.datasource

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import dps.atomic.Operator
import dps.datasource.define.DatasourceDefine
import dps.datasource.define.DatasourceParamDefine


class CommonSource(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val operator: Operator) extends DataSource(sparkSession, sparkConf, operator) {
  override def read() {
    operator.operation();
  }
  def define(): DatasourceDefine = {
    val paramDefines = Map[String, DatasourceParamDefine]()
    val datasourceDefine = new DatasourceDefine("ds.common", paramDefines.toMap)
    datasourceDefine.id = "common_source_define"
    return datasourceDefine
  }
}