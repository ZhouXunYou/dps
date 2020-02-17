package dps.atomic.impl

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine

class RDDStoreHive(override val sparkContext: SparkContext, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    val session = SparkSession.builder.enableHiveSupport().getOrCreate();
    val df = session.createDataFrame(dataset.collectAsList(), dataset.toDF().schema);
    val table = params.get("table").get
    df.write.mode(SaveMode.Append).insertInto(table)
  }
  def define: AtomOperationDefine = {
    val params = Map(
      "table"->new AtomOperationParamDefine("Hive Table Name","Table Name",true,"1")
    )
    val  atomOperation = new AtomOperationDefine("RDD Store Hive","rddStoreHive",null,params.toMap)
    atomOperation.id = "rdd_store_hive"
    return atomOperation
  }
}