package dps.atomic.impl

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine

class AlarmEngine(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {

    val ruleExtends: RDD[Map[String, Any]] = this.pendingData.asInstanceOf[RDD[Map[String, Any]]]

    if (ruleExtends.isEmpty()) {
      println("+------------------------------+")
      println("无规则数据,跳过告警引擎计算")
      println("+------------------------------+")
    } else {
      
      alarmOriginalHandle(ruleExtends, params)

      alarmActiveHandle(ruleExtends, params)
    }

  }

  /**
   * 活动告警条件过滤并存储
   */
  private def alarmActiveHandle(rules: RDD[Map[String, Any]], params: Map[String, String]) = {
    rules.collect().foreach(m => {
      val alarmSql = s"""(
                         select alarm_content,
                                alarm_level,
                                identification_field,
                                alarm_rule_id,
                                count(0) as occur_time 
                           from b_alarm_original 
                           where alarm_rule_id = '${m.get("alarm_rule_id").get}'
                           group by alarm_content,
                                alarm_level,
                                identification_field,
                                alarm_rule_id 
                           having count(0) > ${m.get("occur_count").get}
                          ) as tmpAlarmOriginal""".stripMargin
      val alarmOriginal: Dataset[Row] = this.jdbcQuery(params, alarmSql)

      val activeSql = s"""(
                          select uuid() as id,
                                 tmpAlarmOriginal.alarm_content as alarm_content,
                                 tmpAlarmOriginal.alarm_level as alarm_level,
                                 tmpRuleView.alarm_rule_name as alarm_title,
                                 tmpAlarmOriginal.identification_field as identification_field,
                                 tmpAlarmOriginal.occur_time as occur_time,
                                 tmpRuleView.alarm_rule_id as alarm_rule_id,
                                 now() as create_time
                            from tmpAlarmOriginal inner join tmpRuleView
                            where tmpAlarmOriginal.alarm_rule_id = tmpRuleView.alarm_rule_id
                          ) as tmpAlarmActive""".stripMargin

      val dataset = sparkSession.sqlContext.sql(activeSql);
      val dscount = dataset.count()
      if (dscount > 0) {
        this.store2db(dataset, params, "b_alarm", SaveMode.Append)
      } else {
        println("+------------------------------+")
        println("无数据,跳过存储操作")
        println("+------------------------------+")
      }
    })
  }

  /**
   * 过滤原始告警并存储
   *
   * @param rules
   * @param params
   */
  private def alarmOriginalHandle(rules: RDD[Map[String, Any]], params: Map[String, String]) = {
    rules.collect().foreach(m => {
      val alarmSql = s"""select uuid() as id,'${m.get("alarm_content_expression").get}' as alarm_content,${m.get("alarm_rule_level").get} as alarm_level,'' as identification_field,wtime as occur_time,'${m.get("alarm_rule_id").get}' as alarm_rule_id from alertLBSExtends where ${m.get("conditions").get}"""
      val dataset = sparkSession.sqlContext.sql(alarmSql);
      val dscount = dataset.count()
      if (dscount > 0) {
        this.store2db(dataset, params, "b_alarm_original", SaveMode.Append)
      } else {
        println("+------------------------------+")
        println("无数据,跳过存储操作")
        println("+------------------------------+")
      }
    })
  }

  /**
   * 数据存储到关系型数据库
   *
   * @param dataset
   * @param params
   * @param dbtable
   * @param savemode
   */
  private def store2db(dataset: Dataset[Row], params: Map[String, String], dbtable: String, savemode: SaveMode) = {
    dataset.write.format("jdbc")
      .option("driver", params.get("driver").get)
      .option("url", params.get("url").get)
      .option("dbtable", dbtable)
      .option("user", params.get("user").get)
      .option("password", params.get("password").get)
      .mode(savemode).save();
  }

  /**
   * JDBC查询返回Dataset[Row]
   *
   * @param params
   * @param sql
   * @return
   */
  private def jdbcQuery(params: Map[String, String], sql: String): Dataset[Row] = {
    sparkSession.sqlContext.read.format("jdbc")
      .option("url", params.get("url").get)
      .option("driver", params.get("driver").get)
      .option("dbtable", sql)
      .option("user", params.get("user").get)
      .option("password", params.get("password").get)
      .load();
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "driver" -> new AtomOperationParamDefine("JDBC Driver", "org.postgresql.Driver", true, "1"),
      "url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
      "user" -> new AtomOperationParamDefine("User", "user", true, "1"),
      "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"),
      "originalSql" -> new AtomOperationParamDefine("Original SQL", "select * from dual", true, "3"),
      "activeSql" -> new AtomOperationParamDefine("Active SQL", "select * from dual", true, "3"))
    val atomOperation = new AtomOperationDefine("AlarmEngine", "alarmEngine", "AlarmEngine.flt", params.toMap)
    atomOperation.id = "alarm_engine"
    return atomOperation
  }
}