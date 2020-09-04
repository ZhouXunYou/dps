package dps.atomic.impl

import java.util

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }

import scala.collection.mutable.Map
import java.util.ArrayList

class AlarmEngineOriginal(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

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
    rules.collect().foreach(m => {
      val alarmSql = s"""select * from dual"""
      val dataset = sparkSession.sqlContext.sql(alarmSql);
      val dscount = dataset.count()
      if (dscount > 0) {
        dataset.write.format("jdbc")
          .option("driver", params.get("driver").get)
          .option("url", params.get("url").get)
          .option("dbtable", params.get("alarmTable").get)
          .option("user", params.get("user").get)
          .option("password", params.get("password").get)
          .mode(SaveMode.Append).save();
      } else {
        println("+------------------------------+")
        println("无数据,跳过存储操作")
        println("+------------------------------+")
      }
    })
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
          ) as tmpView""".stripMargin
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
         ) as tmpView""".stripMargin
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
        ) as tmpView""".stripMargin

    val ruleDataset: Dataset[Row] = jdbcQuery(params, ruleSql)
    val identificationDataset: Dataset[Row] = jdbcQuery(params, identificationSql)
    val conditionDataset: Dataset[Row] = jdbcQuery(params, conditionSql)

    val rddWheres: RDD[Tuple2[String, String]] = conditionsSplicing(identificationDataset, conditionDataset)
    
    val identificationField = identificationAssembly(identificationDataset)

    val rddRule = ruleDataset.rdd.map(r => {
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
        "identification_field" -> t._2._2
      )
    })

//    rddRule.join(rddWheres).map(t => {
//      Map(
//        "alarm_rule_id" -> t._1,
//        "aggregate_occur_count" -> t._2._1.get(1),
//        "alarm_content_expression" -> t._2._1.get(2),
//        "alarm_rule_level" -> t._2._1.get(3),
//        "alarm_rule_name" -> t._2._1.get(4),
//        "occur_count" -> t._2._1.get(5),
//        "conditions" -> t._2._2)
//    })
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
        if (StringUtils.isNotBlank(str)) {
          str = str + ","
        }
        str = str + "\"" + row.getAs("field").asInstanceOf[String] + "\":\"" + row.getAs("expression").asInstanceOf[String] + "\""
      })
      Tuple2(v._1, "{"+str+"}")
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
      "alarmTable" -> new AtomOperationParamDefine("Alarm Table Name", "b_alarm", true, "1"))
    val atomOperation = new AtomOperationDefine("AlarmEngine", "alarmEngine", "AlarmEngine.flt", params.toMap)
    atomOperation.id = "alarm_engine"
    return atomOperation
  }
}