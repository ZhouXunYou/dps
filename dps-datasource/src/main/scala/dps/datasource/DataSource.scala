package dps.datasource

import org.apache.spark.SparkContext

import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine
import dps.atomic.Operator
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

abstract class DataSource(val sparkSession: SparkSession, val sparkConf: SparkConf, val operator: Operator) {
  def read()
  def define(): DatasourceDefine
}