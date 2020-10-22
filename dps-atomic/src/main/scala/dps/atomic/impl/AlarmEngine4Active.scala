package dps.atomic.impl

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.ArrayList
import dps.atomic.define.AtomOperationDefine
import org.apache.spark.rdd.RDD
import dps.atomic.define.AtomOperationParamDefine

class AlarmEngine4Active(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val rules: Dataset[Row] = this.pendingData.asInstanceOf[Dataset[Row]]

    if (rules.isEmpty) {
      println("+------------------------------+")
      println("无规则数据,跳过活动告警计算")
      println("+------------------------------+")
    } else {

      val original: RDD[Dataset[Row]] = rules.rdd.map(m => {

        val sql = s"""(select md5(now()::text||random()::text) as id,
                            alarm_content,
                            alarm_level,
                            '${m.getAs("alarm_rule_name").asInstanceOf[String]}' as alarm_title,
                            identification_field,
                            count(0) as occur_count,
														max(occur_time) as  occur_time,
                            alarm_rule_id,
                            now() as create_time
                       from b_alarm_original 
                       where coalesce(alarm_rule_id,'') not in (select distinct coalesce(alarm_rule_id,'') as alarm_rule_id from b_alarm)
                        and alarm_rule_id = '${m.getAs("alarm_rule_id").asInstanceOf[String]}'
                       group by alarm_rule_id,
                            alarm_level,
                            identification_field,
                            alarm_content
                       having count(0) > ${m.getAs("occur_count").asInstanceOf[Integer]}) as tmpAlarmActive""".stripMargin

        sparkSession.sqlContext.read.format("jdbc")
          .option("url", params.get("url").get)
          .option("driver", params.get("driver").get)
          .option("dbtable", sql)
          .option("user", params.get("user").get)
          .option("password", params.get("password").get)
          .load()
      })
    }
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "driver" -> new AtomOperationParamDefine("JDBC Driver", "org.postgresql.Driver", true, "1"),
      "url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
      "user" -> new AtomOperationParamDefine("User", "user", true, "1"),
      "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"))
    val atomOperation = new AtomOperationDefine("AlarmEngine4Active", "alarmEngine4Active", "AlarmEngine4Active.flt", params.toMap)
    atomOperation.id = "alarm_engine_4_active"
    return atomOperation
  }
}