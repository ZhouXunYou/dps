package dps.atomic.impl

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine

class RDDStoreParquet(override val sparkContext: SparkContext, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    val paramPartitionNumValue = params.get("partitionNum").getOrElse(s"${sparkContext.defaultMinPartitions}").asInstanceOf[Int]
    val path = params.get("path").get
    dataset.coalesce(paramPartitionNumValue).write.mode(SaveMode.Append).parquet(path)
  }
  override def define: AtomOperationDefine = {
    val params = Map(
      "partitionNum"->new AtomOperationParamDefine("Partition Size","1",false,"1"),
      "path"->new AtomOperationParamDefine("path","hdfs://${host}:${port}/${warehouse}",true,"1")
    )
    val atomOperation = new AtomOperationDefine("RDD Store Parquet","rddStoreParuet","RDDStoreParquet.flt",params.toMap)
    atomOperation.id = "rdd_store_parquet"
    return atomOperation
  }
}