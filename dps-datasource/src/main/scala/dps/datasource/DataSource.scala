package dps.datasource

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine

abstract class DataSource(val sparkContext: SparkContext, val params: Map[String, String]) {
  def read(): Any
  def define(): DatasourceDefine
}