package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable.Map
import org.apache.spark.SparkConf

class RDDStoreParquet(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    if (dataset != null && dataset.isEmpty) {
      println("+------------------------------+")
      println("无数据,跳过存储操作")
      println("+------------------------------+")
    } else {
      val partitionNum = params.get("partitionNum").getOrElse(sparkSession.sparkContext.defaultMinPartitions.toString()).toInt
      val path = params.get("path").get
      dataset.coalesce(partitionNum.toInt).write.mode(SaveMode.Append).parquet(path)
    }
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