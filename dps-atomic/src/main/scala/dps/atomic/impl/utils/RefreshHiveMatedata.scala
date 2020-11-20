package dps.atomic.impl.store

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction
import org.slf4j.LoggerFactory

class RefreshHiveMatedata(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    val logger = LoggerFactory.getLogger(classOf[RefreshHiveMatedata])

    def doIt(params: Map[String, String]): Any = {
        logger.info(s"refresh ${params.get("warehouse").get}.${params.get("table").get}")
        sparkSession.sqlContext.sql(s"msck repair table ${params.get("warehouse").get}.${params.get("table").get}");
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "warehouse" -> new AtomOperationParamDefine("warehouse.name", "default", true, stringType),
            "table" -> new AtomOperationParamDefine("table.name", "Table Name", true, stringType))
        val atomOperation = new AtomOperationDefine(getClassName, getClassSimpleName, s"utils/${getClassSimpleName}.ftl", params.toMap, classOf[Nothing], classOf[Nothing], classOf[Nothing], classOf[Nothing])
        atomOperation.id = "refresh_hive_matedata"
        return atomOperation
    }
}