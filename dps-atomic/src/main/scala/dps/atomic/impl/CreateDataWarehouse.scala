package dps.atomic.impl
import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine

class CreateDataWarehouse(override val sparkContext:SparkContext, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    return null
  }
  override def define: AtomOperationDefine = {
    val params = Map(
      "warehouseType"->new AtomOperationParamDefine("Warehouse Type",",Hive,Hbase",true,"1"),
      "warehouseName"->new AtomOperationParamDefine("Warehouse","Name",true,"1")    
    )
    val atomOperation = new AtomOperationDefine("Create Data Warehouse","createDataWarehouse","CreateDataWarehouse.flt",params.toMap)
    atomOperation.id = "create_data_warehouse"
    return atomOperation
  }
}