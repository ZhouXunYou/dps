package dps.atomic.impl.rdd

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.Properties
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class RDDRemoveDatabase(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    if (dataset != null && dataset.isEmpty) {
      println("+------------------------------+")
      println("无数据,跳过移除操作")
      println("+------------------------------+")
    } else {
      val props = new Properties
      props.put("user", params.get("user").get)
      props.put("password", params.get("password").get)
      val conn: Connection = DriverManager.getConnection(params.get("url").get, props)
      conn.setAutoCommit(false)
      try {
        val sql = s""""delete from ${params.get("table").get} where ${params.getOrElse("field", "id")} = ?1""".stripMargin
        val stmt: PreparedStatement = conn.prepareStatement(sql)

        dataset.foreach(f => {
          stmt.setString(1, f.getAs("\"".+(params.getOrElse("field", "id")).+("\"")).asInstanceOf[String])
        })

        stmt.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => println(e.printStackTrace())
      } finally {
        conn.close()
      }
    }
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "driver" -> new AtomOperationParamDefine("JDBC Driver", "org.postgresql.Driver", true, "1"),
      "url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
      "user" -> new AtomOperationParamDefine("User", "user", true, "1"),
      "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"),
      "table" -> new AtomOperationParamDefine("Table Name", "Table Name", true, "1"),
      "field" -> new AtomOperationParamDefine("Field Name", "id", true, "1"))
    val atomOperation = new AtomOperationDefine("RDD Remove Database", "rddRemoveDatabase", "RDDRemoveDatabase.flt", params.toMap)
    atomOperation.id = "rdd_remove_database"
    return atomOperation
  }
}