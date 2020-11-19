package dps.atomic.temp

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class DatasetsJoinEachOther(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val leftDataset = params.get("leftVariableKey").get.asInstanceOf[Dataset[Row]]
    val rightDataset = params.get("rightVariableKey").get.asInstanceOf[Dataset[Row]]
    
    val datasetJoin = leftDataset.join(rightDataset, params.get("colum").get)
    datasetJoin.createOrReplaceTempView(params.get("viewName").get)
    this.variables.put(outputVariableKey, datasetJoin);
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "leftVariableKey" -> new AtomOperationParamDefine("Left Dataset", "Left Dataset Variable Key", true, "1"),
      "rightVariableKey" -> new AtomOperationParamDefine("Right Dataset", "Right Dataset Variable Key", true, "1"),
      "colum" -> new AtomOperationParamDefine("Join With Colum", "Join With Colum", true, "1"),
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1")
    )
    val atomOperation = new AtomOperationDefine("Dataset Join", "datasetJoin", "DatasetJoinRDD.flt", params.toMap)
    atomOperation.id = "dataset_join"
    return atomOperation
  }
}