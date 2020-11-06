package dps.atomic.impl.dataset

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class DatasetConvertArray(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val field = params.get("field").get
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]

    val array: Array[String] = dataset.rdd.map(f => {
      f.getAs(field).asInstanceOf[String]
    }).collect()

    this.variables.put(outputVariableKey, array);
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "field" -> new AtomOperationParamDefine("Field", "Field", true, "1"))
    val atomOperation = new AtomOperationDefine("Dataset Convert Array", "datasetConvertArray", "DatasetConvertArray.flt", params.toMap)
    atomOperation.id = "dataset_convert_array"
    return atomOperation
  }
}