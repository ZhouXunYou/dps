package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable.Map

class RDDStoreParquet(override val sparkSession: SparkSession, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends dps.atomic.impl.AbstractAction(sparkSession, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    val partitionNum = params.get("partitionNum").getOrElse(sparkSession.sparkContext.defaultMinPartitions.toString()).toInt
    val path = params.get("path").get
    dataset.coalesce(partitionNum).write.mode(SaveMode.Append).parquet(path)
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "partitionNum" -> new AtomOperationParamDefine("Partition Size", "1", false, "1"),
      "path" -> new AtomOperationParamDefine("path", "hdfs://${host}:${port}/${warehouse}", true, "1")
    )
    val atomOperation = new AtomOperationDefine("RDD Store Parquet", "rddStoreParuet", "RDDStoreParquet.flt", params.toMap)
    atomOperation.id = "rdd_store_parquet"
    return atomOperation
  }
}