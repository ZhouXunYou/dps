package dps.atomic.impl.store

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.hive.HiveContext

class StoreHive(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        val warehouse = params.get("warehouse").get
        val table = params.get("table").get
        dataset.write.mode(SaveMode.Append).insertInto(s"${warehouse}.${table}")
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "warehouse" -> new AtomOperationParamDefine("warehouse.name", "default", true, stringType),
            "table" -> new AtomOperationParamDefine("table.name", "Table Name", true, stringType))
        val atomOperation = new AtomOperationDefine(getClassName, getClassSimpleName, s"store/${getClassSimpleName}.ftl", params.toMap, classOf[Dataset[_]], classOf[Nothing], classOf[Row], classOf[Nothing])
        atomOperation.id = "store_hive"
        return atomOperation
    }
}