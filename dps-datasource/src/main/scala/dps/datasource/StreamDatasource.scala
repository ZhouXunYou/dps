package dps.datasource

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import java.util.Optional
import org.apache.spark.streaming.Seconds
import dps.atomic.Operator

abstract class StreamDatasource(override val sparkContext: SparkContext,override val params: Map[String, String],override val operator:Operator) extends DataSource(sparkContext, params,operator){
  val streamingContext = new StreamingContext(sparkContext, Seconds(params.get("duration").getOrElse("300").toLong))
  def start(){
    streamingContext.start()
    streamingContext.awaitTermination();
  }
}