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

abstract class StreamDatasource(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val operator: Operator) extends DataSource(sparkSession, sparkConf, operator) {
  val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(Optional.ofNullable(sparkConf.get("kafka.duration")).orElse("300").toLong))
  def start(){
    streamingContext.start()
    streamingContext.awaitTermination();
  }
}