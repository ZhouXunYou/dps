package dps.datasource

import org.apache.spark.SparkContext

import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import java.util.Optional

import org.apache.spark.streaming.Seconds
import dps.atomic.Operator
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

abstract class StreamDatasource(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val params: Map[String, String], override val operator: Operator) extends DataSource(sparkSession, sparkConf, params, operator) {
  val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(params.get("duration").getOrElse("60").toLong))
  def start(){
    streamingContext.start()
    streamingContext.awaitTermination();
  }
}