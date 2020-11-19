package dps.atomic.temp

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction

class FilesLoad(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val path = params.get("path").get
    val viewName = params.get("viewName").get
    val dataset = sparkSession.sqlContext.read.load(path);
    dataset.createOrReplaceTempView(viewName);
    this.variables.put(outputVariableKey, dataset);
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "path" -> new AtomOperationParamDefine("Path", "local or distributed file system path", true, "1"),
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1"))
    val atomOperation = new AtomOperationDefine("Files Load", "filesLoad", "FilesLoad.flt", params.toMap)
    atomOperation.id = "Files_Load"
    return atomOperation
  }
}