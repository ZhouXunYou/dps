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
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject

class AlarmRulesAnalysis(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {

    val topicNames: ArrayList[String] = variables.get("topicNames").get.asInstanceOf[ArrayList[String]]
    var topicName = new String
    if (topicNames.size() > 0) {
      topicName = topicNames.get(0)
    }

    val rules = this.ruleAnalysis(params, topicName)

    this.variables.put(outputVariableKey, rules)
  }

  private def ruleAnalysis(params: Map[String, String], topicName: String):RDD[Map[String, Any]] = {
    val rules = this.getRules(params, topicName)
    val identifications = this.getIdentifications(params, topicName)
    val conditions = this.getConditions(params, topicName)

    if (rules.isEmpty || identifications.isEmpty || conditions.isEmpty) {
      println("+------------------------------+")
      println("缺失规则数据,跳过告警规则分析")
      println("+------------------------------+")
      sparkSession.sparkContext.emptyRDD[Map[String, Any]]
    } else {
      val identificationTuple = this.identification(identifications)
      val conditionTuple = this.conditions(identifications, conditions)

      rules.rdd.map(r => {
        (r.getString(0), r)
      }).join(conditionTuple).join(identificationTuple).map(t => {
        Map(
          "alarm_rule_id" -> t._1,
          "aggregate_occur_count" -> t._2._1._1.getAs("aggregate_occur_count").asInstanceOf[Int],
          "alarm_content_expression" -> t._2._1._1.getAs("alarm_content_expression").asInstanceOf[String],
          "alarm_rule_level" -> t._2._1._1.getAs("alarm_rule_level").asInstanceOf[Int],
          "alarm_rule_name" -> t._2._1._1.getAs("alarm_rule_name").asInstanceOf[String],
          "occur_count" -> t._2._1._1.getAs("occur_count").asInstanceOf[Int],
          "alarm_type" -> t._2._1._1.getAs("alarm_type").asInstanceOf[String],
          "alarm_respons_field" -> t._2._1._1.getAs("alarm_respons_field").asInstanceOf[String],
          "left_relation" -> t._2._1._1.getAs("left_relation").asInstanceOf[String],
          "conditions" -> t._2._1._2,
          "identification_field" -> this.jsonAdd(t._2._2, "alarmType", t._2._1._1.getAs("alarm_type").asInstanceOf[String]))
      })
    }
  }

  /**
   * 主识别字段分析
   */
  private def identification(identifications: Dataset[Row]): RDD[Tuple2[String, String]] = {

    identifications.rdd.groupBy(f => {
      f.getAs("alarm_rule_id").asInstanceOf[String]
    }).map(v => {
      var str = new String
      str = this.jsonAdd(str, "isTrue", "1")
      v._2.foreach(row => {
        val field = row.getAs("field").asInstanceOf[String]
        val expression = row.getAs("expression").asInstanceOf[String]

        str = this.jsonAdd(str, field, expression)
      })
      Tuple2(v._1, str)
    })
  }

  /**
   * jsonAdd
   *
   * @param jsonStr
   * @param field
   * @param value
   * @return
   */
  private def jsonAdd(jsonStr: String, field: String, value: Any): String = {
    var obj: JSONObject = new JSONObject
    if (!jsonStr.isEmpty()) {
      obj = JSON.parseObject(jsonStr)
    }
    obj.put(field, value)
    obj.toJSONString()
  }

  /**
   * 条件分析
   */
  private def conditions(identifications: Dataset[Row], conditions: Dataset[Row]): RDD[Tuple2[String, String]] = {

    identifications.rdd.groupBy(row => {
      row.getAs("alarm_rule_id").asInstanceOf[String]
    }).map(v => {
      var map = new HashMap[String, String]
      v._2.foreach(row => {
        if (!"areaType".equals(row.getAs("field").asInstanceOf[String])) {

          map.put(row.getAs("field").asInstanceOf[String], row.getAs("expression").asInstanceOf[String])
        }
      })

      val where = "areaId".+("='").+(map.get("areaId")).+("'")
      //      val where = "areaId".+("='").+(map.get("areaId")).+("' or security_id = '").+(map.get("areaId")).+("'")
      Tuple2(v._1, where)
    }).join(
      conditions.rdd.groupBy(row => {
        row.getAs("alarm_rule_id").asInstanceOf[String]
      }).map(v => {
        var where = new String
        v._2.foreach(row => {
          val field = row.getAs("field").asInstanceOf[String]
          val expression = new java.math.BigDecimal(row.getAs("expression").asInstanceOf[String])
          val comparison = row.getAs("comparison").asInstanceOf[String]

          var logical = " and "
          if (row.getAs("and_or").asInstanceOf[Int] == 0) {
            logical = " or "
          } else {
            logical = " and "
          }

          val current = field.+(comparison).+(expression)
          if (where.isEmpty()) {
            where += current
          } else {
            where += logical.concat(current)
          }
        })
        Tuple2(v._1, where)
      })).map(tuple2 => {
        (tuple2._1, "(".+(tuple2._2._1).+(") and (").+(tuple2._2._2).+(")"))
      })
  }

  /**
   * 获取规则
   */
  private def getRules(params: Map[String, String], topicName: String): Dataset[Row] = {
    val sql = s"""(SELECT
                      	ar.ID AS alarm_rule_id,
                      	COALESCE ( ar.aggregate_occur_count, 0 ) AS aggregate_occur_count,
                      	ar.alarm_content_expression,
                      	ar.alarm_rule_level,
                      	ar.alarm_rule_name,
                      	COALESCE ( ar.occur_count, 0 ) AS occur_count,
                      	ar.alarm_type,
                      	ar.alarm_respons_field,
                      	arr.left_relation 
                      FROM
                      	s_alarm_rule ar
                      	INNER JOIN s_alarm_rule_relation arr ON ar.ID = arr.alarm_rule_id
                      	INNER JOIN s_alarm_rule_identification ari ON ar.ID = ari.alarm_rule_id 
                      WHERE
                      	ar.alarm_rule_status = 1 
                      	AND ari.identification_field = 'areaId' 
                      	AND SUBSTRING ( ari.expression, 7 ) <> '000000' 
                      	AND arr.left_relation = '${topicName}') as tmpView""".stripMargin

    this.jdbcQuery(params, sql)
  }

  /**
   * 获取主识别参数
   */
  private def getIdentifications(params: Map[String, String], topicName: String): Dataset[Row] = {
    val sql = s"""(SELECT
                      	ari.alarm_rule_id,
                      	ari.identification_field AS field,
                      	ari.expression,
                      	ari.ID 
                      FROM
                      	s_alarm_rule_identification ari
                      	INNER JOIN s_alarm_rule ar ON ari.alarm_rule_id = ar.
                      	ID INNER JOIN s_alarm_rule_relation arr ON arr.alarm_rule_id = ar.ID 
                      WHERE
                      	ar.alarm_rule_status = 1 
                      	AND ari.identification_field = 'areaId' 
                      	AND SUBSTRING ( ari.expression, 7 ) <> '000000' 
                      	AND arr.left_relation = '${topicName}') as tmpView""".stripMargin

    this.jdbcQuery(params, sql)
  }

  /**
   * 获取规则条件
   */
  private def getConditions(params: Map[String, String], topicName: String): Dataset[Row] = {
    val sql = s"""(SELECT
                      	arc.alarm_rule_id,
                      	arc.condition_field AS field,
                      	arc.expression,
                      	arc.comparison,
                      	arc.and_or,
                      	arc.ID 
                      FROM
                      	s_alarm_rule_condition arc
                      	INNER JOIN s_alarm_rule ar ON arc.alarm_rule_id = ar.
                      	ID INNER JOIN s_alarm_rule_relation arr ON arr.alarm_rule_id = ar.
                      	ID INNER JOIN s_alarm_rule_identification ari ON ar.ID = ari.alarm_rule_id 
                      WHERE
                      	ar.alarm_rule_status = 1 
                      	AND ari.identification_field = 'areaId' 
                      	AND SUBSTRING ( ari.expression, 7 ) <> '000000' 
                      	AND arr.left_relation = '${topicName}') as tmpView""".stripMargin

    this.jdbcQuery(params, sql)
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
      "ruleSql" -> new AtomOperationParamDefine("Rule SQL", "select * from dual", true, "3"), 
      "identificationSql" -> new AtomOperationParamDefine("Identification SQL", "select * from dual", true, "3"), 
      "conditionSql" -> new AtomOperationParamDefine("Condition SQL", "select * from dual", true, "3"))
    val atomOperation = new AtomOperationDefine("AlarmRulesAnalysis", "alarmRulesAnalysis", "AlarmRulesAnalysis.flt", params.toMap)
    atomOperation.id = "alarm_rules_analysis"
    return atomOperation
  }
}