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

    val sql = s"""(select md5(now()::text||random()::text) as id,
                  				a.alarm_content,
                  				a.alarm_level,
                  				b.alarm_rule_name as alarm_title,
                  				a.identification_field,
                          now() as merge_time,
                  				count(0) as occur_count,
                  				max(a.occur_time) as  occur_time,
                  				a.alarm_rule_id,
                          null as moid,
                          now() as create_time,
                          '{}' as alarm_respons_field
                  from b_alarm_original a
                  inner join s_alarm_rule b
                  on a.alarm_rule_id = b.id
                  where coalesce(a.alarm_rule_id,'') not in (select distinct coalesce(alarm_rule_id,'') as alarm_rule_id from b_alarm)
                  						group by a.alarm_rule_id,
                  										a.alarm_level,
                  										a.identification_field,
                  										a.alarm_content,
                  										b.alarm_rule_name
                  							 having count(0) >= 1) as tmpAlarmOriginal""".stripMargin

    val dataset = sparkSession.sqlContext.read.format("jdbc")
      .option("url", params.get("url").get)
      .option("driver", params.get("driver").get)
      .option("dbtable", sql)
      .option("user", params.get("user").get)
      .option("password", params.get("password").get)
      .load()

    this.variables.put(outputVariableKey, dataset)
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