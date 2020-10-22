package dps.atomic.impl

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.util.Properties

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine

class RDDModifyDatabase(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    if (dataset != null && dataset.isEmpty) {
      println("+------------------------------+")
      println("无数据,跳过变更操作")
      println("+------------------------------+")
    } else {
      val props = new Properties
      props.put("user", params.get("user").get)
      props.put("password", params.get("password").get)
      val conn: Connection = DriverManager.getConnection(params.get("url").get, props)
      conn.setAutoCommit(false)
      try {
        val sql = s"""${params.get("sql").get}""".stripMargin
        val stmt: PreparedStatement = conn.prepareStatement(sql)

        dataset.foreach(f => {
          stmt.setString(1, f.getAs("alarm_content").asInstanceOf[String])
          stmt.setInt(2, f.getAs("alarm_level").asInstanceOf[Integer])
          stmt.setString(3, f.getAs("alarm_title").asInstanceOf[String])
          stmt.setString(4, f.getAs("identification_field").asInstanceOf[String])
          stmt.setLong(5, f.getAs("occur_count").asInstanceOf[Long])
          stmt.setTimestamp(6, f.getAs("merge_time").asInstanceOf[Timestamp])
          stmt.setString(7, f.getAs("alarm_rule_id").asInstanceOf[String])
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
      "url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
      "user" -> new AtomOperationParamDefine("User", "user", true, "1"),
      "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"),
      "sql" -> new AtomOperationParamDefine(
        "SQL",
        """update b_alarm 
                           set alarm_content = ?1, 
                           alarm_level = ?2, 
                           alarm_title = ?3, 
                           identification_field = ?4, 
                           occur_count = ?5, 
                           merge_time = ?6 
                     where alarm_rule_id = ?7
          """, true, "3"),
      "Parameter" -> new AtomOperationParamDefine(
        "Parameter Backfill",
        """dataset.foreach(f => {
          stmt.setString(1, f.getAs("alarm_content").asInstanceOf[String])
          stmt.setInt(2, f.getAs("alarm_level").asInstanceOf[Integer])
          stmt.setString(3, f.getAs("alarm_title").asInstanceOf[String])
          stmt.setString(4, f.getAs("identification_field").asInstanceOf[String])
          stmt.setLong(5, f.getAs("occur_count").asInstanceOf[Long])
          stmt.setTimestamp(6, f.getAs("merge_time").asInstanceOf[Timestamp])
          stmt.setString(7, f.getAs("alarm_rule_id").asInstanceOf[String])
        })
          """, true, "3"))
    val atomOperation = new AtomOperationDefine("RDD Modify Database", "rddModifyDatabase", "RDDModifyDatabase.flt", params.toMap)
    atomOperation.id = "rdd_modify_database"
    return atomOperation
  }
}