package dps.atomic.impl.dataset

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.{ Dataset, Row, SparkSession}
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction

class DatasetFilter(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    if (dataset != null && dataset.isEmpty) {
      println("+------------------------------+")
      println("无数据,跳过数据过滤操作")
      println("+------------------------------+")
    } else {
      variables.put(outputVariableKey, dataset.filter(params.get("filter").get))
    }
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "filter" -> new AtomOperationParamDefine("filter", "", false, "3"))
    val atomOperation = new AtomOperationDefine("Dataset Filter", "datasetFilter", "DatasetFilter.flt", params.toMap)
    atomOperation.id = "dataset_filter"
    return atomOperation
  }
}