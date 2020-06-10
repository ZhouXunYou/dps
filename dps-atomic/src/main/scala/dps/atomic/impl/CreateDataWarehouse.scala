package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map

class CreateDataWarehouse(override val sparkSession: SparkSession, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    return null
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "warehouseType" -> new AtomOperationParamDefine("Warehouse Type", ",Hive,Hbase", true, "1"),
      "warehouseName" -> new AtomOperationParamDefine("Warehouse", "Name", true, "1")
    )
    val atomOperation = new AtomOperationDefine("Create Data Warehouse", "createDataWarehouse", "CreateDataWarehouse.flt", params.toMap)
    atomOperation.id = "create_data_warehouse"
    return atomOperation
  }
}