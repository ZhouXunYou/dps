package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import scala.collection.mutable.Map
import org.apache.spark.SparkConf

class RDDStoreHive(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    val df = sparkSession.createDataFrame(dataset.collectAsList(), dataset.toDF().schema);
    val table = params.get("table").get
    df.write.mode(SaveMode.Append).insertInto(table)
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "table" -> new AtomOperationParamDefine("Hive Table Name", "Table Name", true, "1")
    )
    val atomOperation = new AtomOperationDefine("RDD Store Hive", "rddStoreHive", "RDDStoreHive.flt", params.toMap)
    atomOperation.id = "rdd_store_hive"
    return atomOperation
  }
}