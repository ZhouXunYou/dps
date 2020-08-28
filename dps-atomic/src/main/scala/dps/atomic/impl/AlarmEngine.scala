package dps.atomic.impl

import java.util

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable.Map

class AlarmEngine(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {

    val ruleExtends: RDD[Map[String, Any]] = ruleSplicing(params)

    alarmHandle(ruleExtends, params)

    this.variables.put(outputVariableKey, ruleExtends);
  }
  
  /**
   * 告警处理
   *
   * @param rules
   * @param params
   */
  private def alarmHandle(rules: RDD[Map[String, Any]], params: Map[String, String]) = {
    rules.foreach(m => {
      val alarmSql = s"""select * from dual"""
      val dataset = sparkSession.sqlContext.sql(alarmSql);
      dataset.write.format("jdbc")
        .option("driver", params.get("driver").get)
        .option("url", params.get("url").get)
        .option("dbtable", params.get("alarmTable").get)
        .option("user", params.get("user").get)
        .option("password", params.get("password").get).mode(SaveMode.Append).save();
    })
  }
  
  /**
   * 规则拼接
   *
   * @param params
   */
  private def ruleSplicing(params: Map[String, String]): RDD[Map[String, Any]] = {
    
    val ruleSql =
      s"""(
         |select a.id,
         |       a.aggregate_occur_count,
         |       a.alarm_content_expression,
         |       a.alarm_rule_level,
         |       a.alarm_rule_name,
         |       a.occur_count
         |  from s_alarm_rule a
         |  inner join s_alarm_rule_relation b
         |  on a.id = b.alarm_rule_id
         |  where a.alarm_rule_status = 1
         |) as tmpView""".stripMargin
    val identificationSql =
      s"""(
         |select c.alarm_rule_id,
         |       c.identification_field,
         |       c.expression,
         |       c.id
         |  from s_alarm_rule a
         |  inner join s_alarm_rule_identification c
         |  on a.id = c.alarm_rule_id
         |  where a.alarm_rule_status = 1
         |) as tmpView""".stripMargin
    val conditionSql =
      s"""(
         |select d.alarm_rule_id,
         |       d.condition_field,
         |       d.expression,
         |       d.comparison,
         |       d.and_or,
         |       d.id
         |  from s_alarm_rule a
         |  inner join s_alarm_rule_condition d
         |  on a.id = d.alarm_rule_id
         |  where a.alarm_rule_status = 1
         |) as tmpView""".stripMargin

    val ruleDataset: Dataset[Row] = jdbcQuery(params, ruleSql)
    val identificationDataset: Dataset[Row] = jdbcQuery(params, identificationSql)
    val conditionDataset: Dataset[Row] = jdbcQuery(params, conditionSql)

    val rddWheres: RDD[Tuple2[String, String]] = conditionsSplicing(identificationDataset, conditionDataset)

    val rddRule = ruleDataset.rdd.map(r => {
      (r.getString(0), r)
    })

    rddRule.join(rddWheres).map(t => {
      Map(
        "id" -> t._1,
        "aggregate_occur_count" -> t._2._1.get(1),
        "alarm_content_expression" -> t._2._1.get(2),
        "alarm_rule_level" -> t._2._1.get(3),
        "alarm_rule_name" -> t._2._1.get(4),
        "occur_count" -> t._2._1.get(5),
        "conditions" -> t._2._2
      )
    })
  }

  /**
   * 多条件拼接
   *
   * @param identificationDataset
   * @param conditionDataset
   * @return
   */
  private def conditionsSplicing(identificationDataset: Dataset[Row], conditionDataset: Dataset[Row]): RDD[Tuple2[String, String]] = {

    val identificationRdd: RDD[Tuple2[String, String]] = conditionalAssembly(identificationDataset, false, false)
    val conditionRdd: RDD[Tuple2[String, String]] = conditionalAssembly(conditionDataset, true, true)

    val rddJoin = identificationRdd.join(conditionRdd)
    rddJoin.map(tuple2 => {
      (tuple2._1, ("(" + tuple2._2._1 + ") and (" + tuple2._2._2 + ")"))
    })
  }

  /**
   * Dataset[Row]转RDD[Map[String, String]]
   *
   * @param dataset
   * @param conditionExpr 条件表达式
   * @param logicalExpr   逻辑表达式
   * @return
   */
  private def conditionalAssembly(dataset: Dataset[Row], conditionExpr: Boolean, logicalExpr: Boolean): RDD[Tuple2[String, String]] = {
    dataset.rdd.groupBy(row => {
      row.getString(0)
    }).map(v => {
      var where = new String
      v._2.foreach(row => {
        var condition = "="
        var logical = " and "
        if (conditionExpr) {
          condition = row.getString(3)
        }
        if (logicalExpr) {
          if (row.getInt(4) == 0) {
            logical = " or "
          }
        }

        val current = row.getString(1) + condition + "'" + row.getString(2) + "'"
        if (where.isEmpty()) {
          where += current
        } else {
          where += logical.concat(current)
        }
      })
      Tuple2(v._1, where)
    })
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
//      "ruleSql" -> new AtomOperationParamDefine("Rule SQL", "select a.id,a.aggregate_occur_count,a.alarm_content_expression,a.alarm_rule_level,a.alarm_rule_name,a.occur_count from s_alarm_rule a inner join s_alarm_rule_relation b on a.id = b.alarm_rule_id where a.alarm_rule_status = 1", true, "3"),
//      "identificationSql" -> new AtomOperationParamDefine("Rule Identification SQL", "select c.alarm_rule_id,c.identification_field,c.expression,c.id from s_alarm_rule a inner join s_alarm_rule_identification c on a.id = c.alarm_rule_id where a.alarm_rule_status = 1", true, "3"),
//      "conditionSql" -> new AtomOperationParamDefine("Rule Condition SQL", "select d.alarm_rule_id,d.condition_field,d.expression,d.comparison,d.and_or,d.id from s_alarm_rule a inner join s_alarm_rule_condition d on a.id = d.alarm_rule_id where a.alarm_rule_status = 1", true, "3"),
      "alarmSql" -> new AtomOperationParamDefine("Alarm SQL", "select * from dual", true, "3"),
      //      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1"),
      "alarmTable" -> new AtomOperationParamDefine("Alarm Table Name", "b_alarm", true, "1")
    )
    val atomOperation = new AtomOperationDefine("AlarmEngine", "alarmEngine", "AlarmEngine.flt", params.toMap)
    atomOperation.id = "alarm_engine"
    return atomOperation
  }
}