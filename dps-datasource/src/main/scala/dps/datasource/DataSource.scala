package dps.datasource

import org.apache.spark.SparkContext

import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine
import dps.atomic.Operator
import org.apache.spark.sql.SparkSession


abstract class DataSource(val sparkSession: SparkSession, val params: Map[String, String],val operator:Operator) {
  def read(variableKey:String)
  def define(): DatasourceDefine
}