package dps.atomic.temp

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction

class ExecuteSQL(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val sql = params.get("sql").get
    val viewName = params.get("viewName").get
    val dataset = sparkSession.sqlContext.sql(sql)
    dataset.createOrReplaceTempView(viewName)
    this.variables.put(outputVariableKey, dataset)
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "sql" -> new AtomOperationParamDefine("SQL", "select * from dual", true, "3"),
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1")
    )
    val atomOperation = new AtomOperationDefine("Exceute SQL", "executeSQL", "ExecuteSQL.flt", params.toMap)
    atomOperation.id = "execute_sql"
    return atomOperation
  }
}