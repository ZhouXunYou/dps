package dps.datasource

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import java.util.Optional
import org.apache.spark.streaming.Seconds

abstract class StreamDatasource(override val sparkContext: SparkContext,override val params: Map[String, String]) extends DataSource(sparkContext, params){
  val streamingContext = new StreamingContext(sparkContext, Seconds(params.get("duration").getOrElse("300").toLong))
  def read(): Any
  def define(): DatasourceDefine
  def start(){
    streamingContext.start()
    streamingContext.awaitTermination();
  }
}