package dps.datasource

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine
import dps.atomic.Operator


abstract class DataSource(val sparkContext: SparkContext, val params: Map[String, String],val operator:Operator) {
  def read(variableKey:String)
  def define(): DatasourceDefine
}