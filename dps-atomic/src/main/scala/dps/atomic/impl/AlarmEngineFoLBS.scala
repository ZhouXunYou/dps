package dps.atomic.impl

import java.util

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }

import scala.collection.mutable.Map
import java.util.ArrayList
import java.util.HashMap

class AlarmEngineForLBS(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {

    val ruleExtends: RDD[Map[String, Any]] = ruleSplicing(params)

    if (ruleExtends.isEmpty()) {
      println("+------------------------------+")
      println("无规则数据,跳过告警引擎计算")
      println("+------------------------------+")
    } else {
      
      alarmOriginalHandle(ruleExtends, params)

      alarmActiveHandle(ruleExtends, params)
    }
    
    this.variables.put(outputVariableKey, ruleExtends)
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
            where ar.alarm_rule_status = 1 
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
           where ar.alarm_rule_status = 1 
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
           where ar.alarm_rule_status = 1 
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
      ,"alarmSql" -> new AtomOperationParamDefine("Alarm SQL", "select * from dual", true, "3")
      ,"alarmOriginalSql" -> new AtomOperationParamDefine("Alarm Original SQL", "select * from dual", true, "3")
      ,"ruleSql" -> new AtomOperationParamDefine("Rule SQL", "select * from dual", true, "3")
      ,"identificationSql" -> new AtomOperationParamDefine("Identification SQL", "select * from dual", true, "3")
      ,"conditionSql" -> new AtomOperationParamDefine("Condition SQL", "select * from dual", true, "3")
      )
    val atomOperation = new AtomOperationDefine("AlarmEngineForLBS", "alarmEngineLBS", "AlarmEngineForLBS.flt", params.toMap)
    atomOperation.id = "alarm_engine_lbs"
    return atomOperation
  }
}