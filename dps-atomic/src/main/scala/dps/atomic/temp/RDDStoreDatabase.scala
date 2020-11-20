package dps.atomic.temp

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
//import com.typesafe.scalalogging.Logger
import dps.atomic.impl.AbstractAction
import org.slf4j.LoggerFactory

class RDDStoreDatabase(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

    //  val logger = Logger(this.getClass)
    val logger = LoggerFactory.getLogger(this.getClass)

    def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        if (dataset != null && dataset.isEmpty) {
            logger.info("无数据,跳过存储操作")
            return
        } 
        dataset.write.format("jdbc")
                .option("driver", params.get("driver").get)
                .option("url", params.get("url").get)
                .option("dbtable", params.get("table").get)
                .option("user", params.get("user").get)
                .option("password", params.get("password").get).mode(SaveMode.Append).save()
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "driver" -> new AtomOperationParamDefine("JDBC Driver", "org.postgresql.Driver", true, "1"),
            "url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
            "table" -> new AtomOperationParamDefine("Table Name", "Table Name", true, "1"),
            "user" -> new AtomOperationParamDefine("User", "user", true, "1"),
            "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"))
        val atomOperation = new AtomOperationDefine("RDD Store Database", "rddStoreDatabase", "RDDStoreDatabase.flt", params.toMap, classOf[Nothing], classOf[Nothing], classOf[Nothing], classOf[Nothing])
        atomOperation.id = "rdd_store_database"
        return atomOperation
    }
}