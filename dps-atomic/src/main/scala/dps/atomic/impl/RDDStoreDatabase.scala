package dps.atomic.impl

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine

class RDDStoreDatabase(override val sparkContext: SparkContext, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    dataset.write.format("jdbc")
      .option("driver", params.get("driver").get)
      .option("url", params.get("url").get)
      .option("dbtable", params.get("table").get)
      .option("user", params.get("user").get)
      .option("password", params.get("password").get).mode(SaveMode.Append).save();
  }
  override def define: AtomOperationDefine = {
    val params = Map(
      "driver"->new AtomOperationParamDefine("JDBC Driver","org.postgresql.Driver",true,"1"),
      "url"->new AtomOperationParamDefine("JDBC URL","jdbc:postgresql://ip:port/database",true,"1"),
      "table"->new AtomOperationParamDefine("Table Name","Table Name",true,"1"),
      "user"->new AtomOperationParamDefine("User","user",true,"1"),
      "password"->new AtomOperationParamDefine("Password","*******",true,"1")
    )
    val  atomOperation = new AtomOperationDefine("RDD Store Database","rddStoreDatabase","RDDStoreDatabase.flt",params.toMap)
    atomOperation.id = "rdd_store_database"
    return atomOperation
  }
}