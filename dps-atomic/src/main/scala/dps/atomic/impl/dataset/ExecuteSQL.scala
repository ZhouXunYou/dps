package dps.atomic.impl.dataset

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction

class ExecuteSQL(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

    def doIt(params: Map[String, String]): Any = {
        val sql = params.get("sql").get
        val viewName = params.get("viewName").get
        val dataset = sparkSession.sqlContext.sql(sql)
        dataset.createOrReplaceTempView(viewName)
        this.variables.put(outputVariableKey, dataset)
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "sql" -> new AtomOperationParamDefine("sql", "select * from dual", true, sqlType),
            "viewName" -> new AtomOperationParamDefine("view.name", "View Name", true, stringType))
        val atomOperation = new AtomOperationDefine(getClassName, getClassSimpleName, s"dataset/${getClassSimpleName}.ftl", params.toMap)
        atomOperation.id = "execute_sql"
        return atomOperation
    }
}