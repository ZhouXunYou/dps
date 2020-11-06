package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import java.sql.DriverManager
import java.sql.Connection
import java.util.Properties
import org.apache.spark.sql.Dataset
import java.sql.Timestamp
import org.apache.spark.sql.Row

class JDBCUpdate(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    
    val props = new Properties
    props.put("user", params.get("user").get)
    props.put("password", params.get("password").get)
    val conn: Connection = DriverManager.getConnection(params.get("url").get, props)
    try {
      dataset.foreach(row => {

        val sql = s"""update b_alarm set alarm_content = '${row.getAs("alarm_content").asInstanceOf[String]}',
                                          alarm_level = ${row.getAs("alarm_level").asInstanceOf[Integer]},
                                          alarm_title = '${row.getAs("alarm_title").asInstanceOf[String]}',
                                          identification_field = '${row.getAs("identification_field").asInstanceOf[String]}',
                                          occur_count = ${row.getAs("occur_count").asInstanceOf[Long]},
                                          merge_time = '${row.getAs("merge_time").asInstanceOf[Timestamp]}'
                                    where alarm_rule_id = '${row.getAs("alarm_rule_id").asInstanceOf[String]}'"""
        conn.createStatement().executeUpdate(sql)
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    } finally {
      conn.close()
    }
  }

  override def define: AtomOperationDefine = {
    val params = Map(
    	"url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
      "user" -> new AtomOperationParamDefine("User", "user", true, "1"),
      "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"),
      "JDBCStatement" -> new AtomOperationParamDefine("JDBC statement", 
          """
            update b_alarm set alarm_content = '${row.getAs("alarm_content").asInstanceOf[String]}',
                               alarm_level = ${row.getAs("alarm_level").asInstanceOf[Integer]},
                               alarm_title = '${row.getAs("alarm_title").asInstanceOf[String]}',
                               identification_field = '${row.getAs("identification_field").asInstanceOf[String]}',
                               occur_count = ${row.getAs("occur_count").asInstanceOf[Long]},
                               merge_time = '${row.getAs("merge_time").asInstanceOf[Timestamp]}'
                         where alarm_rule_id = '${row.getAs("alarm_rule_id").asInstanceOf[String]}'
          """, true, "3")
    )
    val atomOperation = new AtomOperationDefine("JDBC Update", "jdbcUpdate", "JDBCUpdate.flt", params.toMap)
    atomOperation.id = "jdbc_update"
    return atomOperation
  }
}