package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map

class ExecuteSQL(override val sparkSession: SparkSession, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val sql = params.get("sql").get
    val viewName = params.get("viewName").get
    val dataset = sparkSession.sqlContext.sql(sql);
    dataset.createOrReplaceTempView(viewName);
    this.variables.put(outputVariableKey, dataset);
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "sql" -> new AtomOperationParamDefine("SQL", "select * from dual", true, "1"),
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1")
    )
    val atomOperation = new AtomOperationDefine("Exceute SQL", "executeSQL", "ExecuteSQL.flt", params.toMap)
    atomOperation.id = "execute_sql"
    return atomOperation
  }
}