package dps.atomic.impl.alarm

import java.util.ArrayList
import java.util.HashMap
import scala.collection.mutable.Map
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

class AlarmRuleAnalysis(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {

    val ruleExtends: RDD[Map[String, Any]] = ruleSplicing(params)

    this.variables.put(outputVariableKey, ruleExtends);
  }

  /**
   * 规则拼接
   *
   * @param params
   */
  private def ruleSplicing(params: Map[String, String]): RDD[Map[String, Any]] = {

    val topicNames: ArrayList[String] = variables.get("topicNames").get.asInstanceOf[ArrayList[String]]
    var topicName = new String
    if (topicNames.size() > 0) {
      topicName = topicNames.get(0)
    }

    val ruleSql =
      s"""(
          select ar.id as alarm_rule_id, 
                 ar.aggregate_occur_count, 
                 ar.alarm_content_expression, 
                 ar.alarm_rule_level, 
                 ar.alarm_rule_name, 
                 ar.occur_count, 
                 ar.alarm_type, 
                 arr.left_relation 
            from s_alarm_rule ar 
            inner join s_alarm_rule_relation arr 
            on ar.id = arr.alarm_rule_id 
            inner join s_alarm_rule_identification ari 
            on ar.id = ari.alarm_rule_id 
            where ar.alarm_rule_status = 1 
            and ari.identification_field = 'areaId' 
            and substring(ari.expression, 7) <> '000000' 
            and arr.left_relation = '${topicName}'
          ) as tmpRuleView""".stripMargin

    val rules: Dataset[Row] = this.jdbcQuery(params, ruleSql)

    val identificationSql =
      s"""(
         select ari.alarm_rule_id,
                ari.identification_field as field,
                ari.expression,
                ari.id
           from s_alarm_rule_identification ari 
           inner join s_alarm_rule ar 
           on ari.alarm_rule_id = ar.id 
           inner join s_alarm_rule_relation arr 
           on arr.alarm_rule_id = ar.id 
           inner join s_alarm_rule_identification ari 
           on ar.id = ari.alarm_rule_id 
           where ar.alarm_rule_status = 1 
           and ari.identification_field = 'areaId' 
           and substring(ari.expression, 7) <> '000000' 
           and arr.left_relation = '${topicName}'
         ) as tmpIdentifacationView""".stripMargin
    val conditionSql =
      s"""(
         select arc.alarm_rule_id,
                arc.condition_field as field,
                arc.expression,
                arc.comparison,
                arc.and_or,
                arc.id
           from s_alarm_rule_condition arc 
           inner join s_alarm_rule ar 
           on arc.alarm_rule_id = ar.id 
           inner join s_alarm_rule_relation arr 
           on arr.alarm_rule_id = ar.id 
           inner join s_alarm_rule_identification ari 
           on ar.id = ari.alarm_rule_id 
           where ar.alarm_rule_status = 1 
           and ari.identification_field = 'areaId' 
           and substring(ari.expression, 7) <> '000000' 
           and arr.left_relation = '${topicName}'
        ) as tmpConditionView""".stripMargin

    val identifications: Dataset[Row] = this.jdbcQuery(params, identificationSql)
    val conditions: Dataset[Row] = this.jdbcQuery(params, conditionSql)

    val rddWheres: RDD[Tuple2[String, String]] = conditionsSplicing(identifications, conditions)

    val identificationField = identificationAssembly(identifications)

    val rddRule = rules.rdd.map(r => {
      (r.getString(0), r)
    })

    rddRule.join(rddWheres).join(identificationField).map(t => {
      Map(
        "alarm_rule_id" -> t._1,
        "aggregate_occur_count" -> t._2._1._1.get(1),
        "alarm_content_expression" -> t._2._1._1.get(2),
        "alarm_rule_level" -> t._2._1._1.get(3),
        "alarm_rule_name" -> t._2._1._1.get(4),
        "occur_count" -> t._2._1._1.get(5),
        "conditions" -> t._2._1._2,
        "identification_field" -> t._2._2)
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

    val identificationRdd: RDD[Tuple2[String, String]] = identification2Condition(identificationDataset)
    val conditionRdd: RDD[Tuple2[String, String]] = conditionalAssembly(conditionDataset)

    val rddJoin = identificationRdd.join(conditionRdd)
    rddJoin.map(tuple2 => {
      (tuple2._1, ("(" + tuple2._2._1 + ") and (" + tuple2._2._2 + ")"))
    })
  }

  /**
   * identification_field属性拼接
   *
   * @param dataset
   * @return
   */
  private def identificationAssembly(dataset: Dataset[Row]): RDD[Tuple2[String, String]] = {
    dataset.rdd.groupBy(row => {
      row.getAs("alarm_rule_id").asInstanceOf[String]
    }).map(v => {
      var str = new String
      v._2.foreach(row => {
        val field = row.getAs("field").asInstanceOf[String]
        val expression = row.getAs("expression").asInstanceOf[String]

        if (StringUtils.isNotBlank(str)) {
          str = str + ","
        }
        str = str + "\"" + field + "\":\"" + expression + "\""
      })
      Tuple2(v._1, "{" + str + "}")
    })
  }

  /**
   * identification2Condition
   *
   * @param dataset
   * @return
   */
  private def identification2Condition(dataset: Dataset[Row]): RDD[Tuple2[String, String]] = {
    dataset.rdd.groupBy(row => {
      row.getAs("alarm_rule_id").asInstanceOf[String]
    }).map(v => {
      var field = new String

      var map = new HashMap[String, String]
      v._2.foreach(row => {
        map.put(row.getAs("field").asInstanceOf[String], row.getAs("expression").asInstanceOf[String])
      })

      if ("REGION_TYPE_AREA".equals(map.get("areaType"))) {
        field = "areaId"
      } else if ("REGION_TYPE_AREA".equals(map.get("areaType"))) {
        field = "guaranteeAreaId"
      }

      val where = field.+("='").+(map.get("areaId")).+("'")

      Tuple2(v._1, where)
    })
  }

  /**
   * Dataset[Row]转RDD[Map[String, String]]
   *
   * @param dataset
   * @return
   */
  private def conditionalAssembly(dataset: Dataset[Row]): RDD[Tuple2[String, String]] = {
    dataset.rdd.groupBy(row => {
      row.getAs("alarm_rule_id").asInstanceOf[String]
    }).map(v => {
      var where = new String
      v._2.foreach(row => {
        val field = row.getAs("field").asInstanceOf[String]
        val expression = row.getAs("expression").asInstanceOf[String]
        val comparison = row.getAs("comparison").asInstanceOf[String]

        var logical = " and "
        if (row.getAs("and_or").asInstanceOf[Int] == 0) {
          logical = " or "
        } else {
          logical = " and "
        }

        val current = field + comparison + "'" + expression + "'"
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
      "driver" -> new AtomOperationParamDefine("JDBC Driver", "org.postgresql.Driver", true, "1")
      ,"url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1")
      ,"user" -> new AtomOperationParamDefine("User", "user", true, "1")
      ,"password" -> new AtomOperationParamDefine("Password", "*******", true, "1")
      ,"ruleSql" -> new AtomOperationParamDefine("Rule SQL", "select * from dual", true, "3")
      ,"identificationSql" -> new AtomOperationParamDefine("Identification SQL", "select * from dual", true, "3")
      ,"conditionSql" -> new AtomOperationParamDefine("Condition SQL", "select * from dual", true, "3")
      )
    val atomOperation = new AtomOperationDefine("AlarmRuleAnalysis", "alarmRuleAnalysis", "AlarmRuleAnalysis.flt", params.toMap)
    atomOperation.id = "alarm_rule_analysis"
    return atomOperation
  }
}