package dps.atomic.impl

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.util.ArrayList
import java.util.HashMap
import java.util.Properties

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine

class AlertClear(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val topicNames: ArrayList[String] = variables.get("topicNames").get.asInstanceOf[ArrayList[String]]
    var topicName = new String
    if (topicNames.size() > 0) {
      topicName = topicNames.get(0)
    }

    val ruleExtends: RDD[Map[String, Any]] = this.ruleAnalysis(params, topicName)
    if (ruleExtends.isEmpty()) {
      println("+------------------------------+")
      println("无规则数据,跳过消警分析计算")
      println("+------------------------------+")
    } else {

      this.clearHandle(ruleExtends, params)
    }

    this.variables.put(outputVariableKey, ruleExtends)

  }

  /**
   * 活动告警条件过滤并存储
   */
  private def clearHandle(rules: RDD[Map[String, Any]], params: Map[String, String]) = {
    rules.collect().foreach(m => {

      val sql = "(select min(occur_time) as start_time,max(occur_time) as end_time from b_alarm_original where alarm_rule_id = '".+(m.get("alarm_rule_id").get).+("' and identification_field::jsonb ->>'alarmType'='").+(m.get("alarm_type").get).+("') as tmpView")

      val timeInterval = this.jdbcQuery(params.get("pgUrl").get, params.get("pgDriver").get, params.get("pgUser").get, params.get("pgPassword").get, sql)

      if (timeInterval.isEmpty) {
        println("+------------------------------+")
        println("当前规则无告警数据,跳过清除告警分析计算操作")
        println("+------------------------------+")
      } else {

        val startTime = timeInterval.first().getAs("start_time").asInstanceOf[String]

        val endTime = timeInterval.first().getAs("end_time").asInstanceOf[String]

        val sql2 = "(select distinct a_time,area_id from b_alarm_tower where a_time >= '".+(startTime).+("' and a_time <= '").+(endTime).+("' and r_time <= now()) as tmpView")

        val recoverableOriginal = this.jdbcQuery(params.get("impalaUrl").get, params.get("impalaDriver").get, params.get("impalaUser").get, params.get("impalaPassword").get, sql2)

        var originalClear: Dataset[Row] = sparkSession.emptyDataFrame
        recoverableOriginal.foreach(row => {
          val sql = "(select * from b_alarm_original where alarm_rule_id = '".+(m.get("alarm_rule_id").get).+("' and identification_field::jsonb ->>'alarmType' = '").+(m.get("alarm_type").get).+(" and identification_field::jsonb ->> 'areaId' = '").+(row.getAs("region").asInstanceOf[String]).+("' and occur_time = '").+(row.getAs("atime").asInstanceOf[String]).+("') as tmpView")

          val data = this.jdbcQuery(params.get("pgUrl").get, params.get("pgDriver").get, params.get("pgUser").get, params.get("pgPassword").get, sql)
          originalClear.unionAll(data)
        })

        var conn: Connection = null
        conn.setAutoCommit(false)
        val props = new Properties
        props.put("user", params.get("user").get)
        props.put("password", params.get("password").get)
        conn = DriverManager.getConnection(params.get("url").get, props)
        var originalIds = new ArrayList[String]

        try {
          // 原始告警数据移到历史详情中
          val sqlInsert = "insert into b_alarm_history_detail (id, alarm_id, alarm_rule_id, alarm_content, alarm_level, identification_field, occur_time) values (md5(random()::text || now()::text),?,?,?,?,?,?)"
          var stmt: PreparedStatement = conn.prepareStatement(sqlInsert)
          originalClear.foreach(row => {
            originalIds.add(row.getAs("id").asInstanceOf[String])
            stmt.setString(1, row.getAs("id").asInstanceOf[String])
            stmt.setString(2, row.getAs("alarm_rule_id").asInstanceOf[String])
            stmt.setString(3, row.getAs("alarm_content").asInstanceOf[String])
            stmt.setInt(4, row.getAs("alarm_level").asInstanceOf[Int])
            stmt.setString(5, row.getAs("identification_field").asInstanceOf[String])
            stmt.setTimestamp(6, row.getAs("occur_time").asInstanceOf[Timestamp])
          })

          stmt.executeBatch()
          conn.commit()

          // 删除原始告警数据
          val sqlDel = "delete from b_alarm_original where id = ?1"
          stmt = conn.prepareStatement(sqlDel)
          val numCount = originalIds.size()
          for (num <- 0 to numCount) {
            stmt.setString(1, originalIds.get(num))
          }
          stmt.executeBatch()
          conn.commit()
        } catch {
          case e: Exception => println(e.printStackTrace())
        } finally {
          conn.close()
        }

        // 处理活动告警
        val sqlActiveCurrent = "select alarm_rule_id from b_alarm_original where coalesce(alarm_rule_id,'') not in (select distinct coalesce(alarm_rule_id,'') as alarm_rule_id from b_alarm) and alarm_rule_id = '".+(m.get("alarm_rule_id").get).+("' group by alarm_rule_id having count(0) > ").+(m.get("occur_count").get)

        val alarmRuleIdExcept = this.jdbcQuery(params.get("pgUrl").get, params.get("pgDriver").get, params.get("pgUser").get, params.get("pgPassword").get, sqlActiveCurrent)

        var exceptAlarmRuleIds = new String
        alarmRuleIdExcept.foreach(row => {
          exceptAlarmRuleIds.+("','").+(row.getAs("alarm_rule_id").asInstanceOf[String])
        })
        exceptAlarmRuleIds = "'".+(exceptAlarmRuleIds).+("'")
        val sqlActiveClear = "select * from b_alarm where alarm_rule_id in (".+(exceptAlarmRuleIds).+(")")
        val activeClear = this.jdbcQuery(params.get("pgUrl").get, params.get("pgDriver").get, params.get("pgUser").get, params.get("pgPassword").get, sqlActiveClear)

        var activeIds = new ArrayList[String]
        try {
          // 原始告警数据移到历史详情中
          val sqlInsert = "insert into b_alarm_history (id, delete_time, alarm_content, alarm_level, alarm_title, identification_field, merge_time, occur_count, occur_time, alarm_id, alarm_rule_id, moid, creat_time, alarm_respons_field) values (md5(random()::text || now()::text),now(),?,?,?,?,?,?,?,?,?,?,?,?)"
          var stmt: PreparedStatement = conn.prepareStatement(sqlInsert)
          activeClear.foreach(row => {
            activeIds.add(row.getAs("id").asInstanceOf[String])
            stmt.setString(1, row.getAs("alarm_content").asInstanceOf[String])
            stmt.setInt(2, row.getAs("alarm_level").asInstanceOf[Int])
            stmt.setString(3, row.getAs("alarm_title").asInstanceOf[String])
            stmt.setString(4, row.getAs("identification_field").asInstanceOf[String])
            stmt.setTimestamp(5, row.getAs("merge_time").asInstanceOf[Timestamp])
            stmt.setInt(6, row.getAs("occur_count").asInstanceOf[Int])
            stmt.setTimestamp(7, row.getAs("occur_time").asInstanceOf[Timestamp])
            stmt.setString(8, row.getAs("id").asInstanceOf[String])
            stmt.setString(9, row.getAs("alarm_rule_id").asInstanceOf[String])
            stmt.setString(10, row.getAs("moid").asInstanceOf[String])
            stmt.setTimestamp(11, row.getAs("create_time").asInstanceOf[Timestamp])
            stmt.setString(12, row.getAs("alarm_respons_field").asInstanceOf[String])
          })

          stmt.executeBatch()
          conn.commit()

          // 删除活动告警数据
          val sqlDel = "delete from b_alarm where id = ?1"
          stmt = conn.prepareStatement(sqlDel)
          val numCount = activeIds.size()
          for (num <- 0 to numCount) {
            stmt.setString(1, activeIds.get(num))
          }
          stmt.executeBatch()
          conn.commit()
        } catch {
          case e: Exception => println(e.printStackTrace())
        } finally {
          conn.close()
        }

        /*originalClear.foreachPartition(iter => {
          try {
            // 原始告警数据移到历史详情中
            val sqlInsert = "insert into b_alarm_history_detail (id, alarm_id, alarm_rule_id, alarm_content, alarm_level, identification_field, occur_time) values (md5(random()::text || now()::text),?,?,?,?,?,?)"
            var stmt: PreparedStatement = conn.prepareStatement(sqlInsert)
            iter.foreach(row => {
              originalIds.add(row.getAs("alarm_id").asInstanceOf[String])
              stmt.setString(1, row.getAs("alarm_id").asInstanceOf[String])
              stmt.setString(2, row.getAs("alarm_rule_id").asInstanceOf[String])
              stmt.setString(3, row.getAs("alarm_content").asInstanceOf[String])
              stmt.setString(4, row.getAs("alarm_level").asInstanceOf[String])
              stmt.setString(5, row.getAs("identification_field").asInstanceOf[String])
              stmt.setTimestamp(6, row.getAs("occur_time").asInstanceOf[Timestamp])
            })

            stmt.executeBatch()
            conn.commit()

            // 删除原始告警数据
            val sqlDel = "delete from b_alarm_original where id in(?1)"
            stmt = conn.prepareStatement(sqlDel)
            stmt.execute()
            conn.commit()
          } catch {
            case e: Exception => println(e.printStackTrace())
          } finally {
            conn.close()
          }
        })*/
      }
    })
  }

  /**
   * 规则分析
   */
  private def ruleAnalysis(params: Map[String, String], topicName: String): RDD[Map[String, Any]] = {
    val rules = this.getRules(params, topicName)
    val identifications = this.getIdentifications(params, topicName)
    val conditions = this.getConditions(params, topicName)

    if (rules.isEmpty || identifications.isEmpty || conditions.isEmpty) {
      println("+------------------------------+")
      println("缺失规则数据,跳过告警规则分析")
      println("+------------------------------+")
      sparkSession.sparkContext.emptyRDD[Map[String, Any]]
    } else {
      rules.createOrReplaceTempView("tmpRuleView")

      val rddConditions: RDD[Tuple2[String, String]] = this.conditionsSplicing(identifications, conditions)

      val identificationField = identifications.rdd.groupBy(row => {
        row.getAs("alarm_rule_id").asInstanceOf[String]
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

      rules.rdd.map(r => {
        (r.getString(0), r)
      }).join(rddConditions).join(identificationField).map(t => {
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
   * 多条件拼接
   */
  private def conditionsSplicing(identifications: Dataset[Row], conditions: Dataset[Row]): RDD[Tuple2[String, String]] = {
    identifications.rdd.groupBy(row => {
      row.getAs("alarm_rule_id").asInstanceOf[String]
    }).map(v => {
//      var field = new String

      var map = new HashMap[String, String]
      v._2.foreach(row => {
        if (!"areaType".equals(row.getAs("field").asInstanceOf[String])) {
          
        	map.put(row.getAs("field").asInstanceOf[String], row.getAs("expression").asInstanceOf[String])
        }
      })

//      if ("REGION_TYPE_AREA".equals(map.get("areaType"))) {
//        field = "areaId"
//      } else if ("REGION_TYPE_AREA".equals(map.get("areaType"))) {
//        field = "guaranteeAreaId"
//      }

      val where = "areaId".+("='").+(map.get("areaId")).+("' or security_id = '").+(map.get("areaId")).+("'")

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
    val sql = "(".+("select ar.id as alarm_rule_id, coalesce(ar.aggregate_occur_count, 0) as aggregate_occur_count, ar.alarm_content_expression, ar.alarm_rule_level, ar.alarm_rule_name, coalesce(ar.occur_count, 0) as occur_count, ar.alarm_type, ar.alarm_respons_field, arr.left_relation from s_alarm_rule ar inner join s_alarm_rule_relation arr on ar.id = arr.alarm_rule_id where ar.alarm_rule_status = 1 and arr.left_relation = '").+(topicName).+("') as tmpView")

    this.jdbcQuery(params.get("pgUrl").get, params.get("pgDriver").get, params.get("pgUser").get, params.get("pgPassword").get, sql)
  }

  /**
   * 获取主识别参数
   */
  private def getIdentifications(params: Map[String, String], topicName: String): Dataset[Row] = {
    val sql = "(".+("select ari.alarm_rule_id, ari.identification_field as field, ari.expression, ari.id from s_alarm_rule_identification ari inner join s_alarm_rule ar on ari.alarm_rule_id = ar.id inner join s_alarm_rule_relation arr on arr.alarm_rule_id = ar.id where ar.alarm_rule_status = 1 and arr.left_relation = '").+(topicName).+("') as tmpView")

    this.jdbcQuery(params.get("pgUrl").get, params.get("pgDriver").get, params.get("pgUser").get, params.get("pgPassword").get, sql)
  }

  /**
   * 获取规则条件
   */
  private def getConditions(params: Map[String, String], topicName: String): Dataset[Row] = {
    val sql = "(".+("select arc.alarm_rule_id, arc.condition_field as field, arc.expression, arc.comparison, arc.and_or, arc.id from s_alarm_rule_condition arc inner join s_alarm_rule ar on arc.alarm_rule_id = ar.id inner join s_alarm_rule_relation arr on arr.alarm_rule_id = ar.id where ar.alarm_rule_status = 1 and arr.left_relation = '").+(topicName).+("') as tmpView")

    this.jdbcQuery(params.get("pgUrl").get, params.get("pgDriver").get, params.get("pgUser").get, params.get("pgPassword").get, sql)
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
      .option("driver", params.get("pgDriver").get)
      .option("url", params.get("pgUrl").get)
      .option("dbtable", dbtable)
      .option("user", params.get("pgUser").get)
      .option("password", params.get("pgPassword").get)
      .mode(savemode).save();
  }

  /**
   * JDBC查询返回Dataset[Row]
   *
   * @param params
   * @param sql
   * @return
   */
  private def jdbcQuery(url: String, driver: String, user: String, password: String, sql: String): Dataset[Row] = {
    sparkSession.sqlContext.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("dbtable", sql)
      .option("user", user)
      .option("password", password)
      .load();
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

  override def define: AtomOperationDefine = {
    val params = Map(
      "pgDriver" -> new AtomOperationParamDefine("Real JDBC Driver", "org.postgresql.Driver", true, "1"),
      "pgUrl" -> new AtomOperationParamDefine("Real JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
      "pgUser" -> new AtomOperationParamDefine("Real User", "user", true, "1"),
      "pgPassword" -> new AtomOperationParamDefine("Real Password", "*******", true, "1"),
      "impalaDriver" -> new AtomOperationParamDefine("Impala JDBC Driver", "com.cloudera.impala.jdbc41.Driver", true, "1"),
      "impalaUrl" -> new AtomOperationParamDefine("Impala JDBC URL", "jdbc:impala://ip:port/database", true, "1"),
      "impalaUser" -> new AtomOperationParamDefine("Impala User", "user", true, "1"),
      "impalaPassword" -> new AtomOperationParamDefine("Impala Password", "*******", true, "1"))
    val atomOperation = new AtomOperationDefine("AlertClear", "alertClear", "AlertClear.flt", params.toMap)
    atomOperation.id = "alert_clear"
    return atomOperation
  }
}