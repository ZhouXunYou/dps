package dps.atomic.impl.alarm

import java.util.ArrayList
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
import java.util.HashMap
import dps.atomic.impl.AbstractAction
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

class AlertEngine(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val topicNames: ArrayList[String] = variables.get("topicNames").get.asInstanceOf[ArrayList[String]]
    var topicName = new String
    if (topicNames.size() > 0) {
      topicName = topicNames.get(0)
    }

    val ruleExtends: RDD[Map[String, Any]] = this.ruleAnalysis(params, topicName)
    if (ruleExtends.isEmpty()) {
      println("+------------------------------+")
      println("无规则数据,跳过告警引擎计算")
      println("+------------------------------+")
    } else {

      this.alarmOriginalHandle(ruleExtends, params)

      this.alarmActive(params)
//      this.alarmActiveHandle(ruleExtends, params)
    }

    this.variables.put(outputVariableKey, ruleExtends)

  }

  /**
   * 活动告警条件过滤并存储
   */
//  private def alarmActiveHandle(rules: RDD[Map[String, Any]], params: Map[String, String]) = {
//    rules.collect().foreach(m => {
//      this.alarmActive(params, m)
//      this.updateAlarmActive(params, m)
//    })
//  }

  /**
   * 活动告警分析
   */
  private def alarmActive(params: Map[String, String]) {
    println("active alarm handle")
    // 新告警过滤
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

    val alarms: Dataset[Row] = this.jdbcQuery(params, sql)
    alarms.show()
    println("active alarm")
    if (alarms.isEmpty) {
      println("+------------------------------+")
      println("活动告警分析无数据,跳过存储操作")
      println("+------------------------------+")
    } else {
      this.store2db(alarms, params, "b_alarm", SaveMode.Append)
    }
  }

  /**
   * 分析并更新已存在活动告警
   */
//  private def updateAlarmActive(params: Map[String, String], m: Map[String, Any]) {
//    // 更新已存在告警的数据
//    val sql = s"""(select ori.alarm_rule_id,
//                            ori.alarm_content,
//                    			  ori.alarm_level,
//                    			  '${m.get("alarm_rule_name").get}' as alarm_title,
//                    			  ori.identification_field,
//                    			  count(0) as occur_count,
//                    			  max(ori.occur_time) as  merge_time
//                       from b_alarm_original ori
//                       inner join b_alarm al on coalesce(ori.alarm_rule_id,'') = coalesce(al.alarm_rule_id,'')
//                       where ori.alarm_rule_id = '${m.get("alarm_rule_id").get}'
//                       group by ori.alarm_rule_id,
//                             ori.alarm_level,
//                    			   ori.identification_field,
//                    			   ori.alarm_content
//                       having count(0) > ${m.get("occur_count").get}
//                  ) as tmpView""".stripMargin
//
//    val alarm: Dataset[Row] = this.jdbcQuery(params, sql)
//
//    var conn: Connection = null
//    val props = new Properties
//    props.put("user", params.get("user").get)
//    props.put("password", params.get("password").get)
//    alarm.foreachPartition(iter => {
//      try {
//        conn = DriverManager.getConnection(params.get("url").get, props)
//        iter.foreach(row => {
//          val alarm_rule_id = row.getAs("alarm_rule_id").asInstanceOf[String]
//          val alarm_content = row.getAs("alarm_content").asInstanceOf[String]
//          val alarm_level = row.getAs("alarm_level").asInstanceOf[Integer]
//          val alarm_title = row.getAs("alarm_title").asInstanceOf[String]
//          val identification_field = row.getAs("identification_field").asInstanceOf[String]
//          val occur_count = row.getAs("occur_count").asInstanceOf[Long]
//          val merge_time = row.getAs("merge_time").asInstanceOf[Timestamp]
//
//          val uSql = s"""update b_alarm set alarm_content = '$alarm_content',
//                                            alarm_level = $alarm_level,
//                                            alarm_title = '$alarm_title',
//                                            identification_field = '$identification_field',
//                                            occur_count = $occur_count,
//                                            merge_time = '$merge_time'
//                                            where alarm_rule_id = '$alarm_rule_id'"""
//          conn.createStatement().executeUpdate(uSql)
//        })
//      } catch {
//        case e: Exception => println(e.printStackTrace())
//      } finally {
//        conn.close()
//      }
//    })
//  }

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
      if (dataset.isEmpty) {
        println("+------------------------------+")
        println("原始告警分析无数据,跳过存储操作")
        println("+------------------------------+")
      } else {
        this.store2db(dataset, params, "b_alarm_original", SaveMode.Append)
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
    val sql = "(".+("select ar.id as alarm_rule_id, coalesce(ar.aggregate_occur_count, 0) as aggregate_occur_count, ar.alarm_content_expression, ar.alarm_rule_level, ar.alarm_rule_name, coalesce(ar.occur_count, 0) as occur_count, ar.alarm_type, ar.alarm_respons_field, arr.left_relation from s_alarm_rule ar inner join s_alarm_rule_relation arr on ar.id = arr.alarm_rule_id inner join s_alarm_rule_identification ari on ar.id = ari.alarm_rule_id where ar.alarm_rule_status = 1 and ari.identification_field = 'areaId' and substring(ari.expression, 7) <> '000000' and arr.left_relation = '").+(topicName).+("') as tmpView")

    this.jdbcQuery(params, sql)
  }

  /**
   * 获取主识别参数
   */
  private def getIdentifications(params: Map[String, String], topicName: String): Dataset[Row] = {
    val sql = "(".+("select ari.alarm_rule_id, ari.identification_field as field, ari.expression, ari.id from s_alarm_rule_identification ari inner join s_alarm_rule ar on ari.alarm_rule_id = ar.id inner join s_alarm_rule_relation arr on arr.alarm_rule_id = ar.id  where ar.alarm_rule_status = 1 and ari.identification_field = 'areaId' and substring(ari.expression, 7) <> '000000' and arr.left_relation = '").+(topicName).+("') as tmpView")
    
    this.jdbcQuery(params, sql)
  }

  /**
   * 获取规则条件
   */
  private def getConditions(params: Map[String, String], topicName: String): Dataset[Row] = {
    val sql = "(".+("select arc.alarm_rule_id, arc.condition_field as field, arc.expression, arc.comparison, arc.and_or, arc.id from s_alarm_rule_condition arc inner join s_alarm_rule ar on arc.alarm_rule_id = ar.id inner join s_alarm_rule_relation arr on arr.alarm_rule_id = ar.id inner join s_alarm_rule_identification ari on ar.id = ari.alarm_rule_id where ar.alarm_rule_status = 1 and ari.identification_field = 'areaId' and substring(ari.expression, 7) <> '000000' and arr.left_relation = '").+(topicName).+("') as tmpView")

    this.jdbcQuery(params, sql)
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
      "driver" -> new AtomOperationParamDefine("JDBC Driver", "org.postgresql.Driver", true, "1"),
      "url" -> new AtomOperationParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database", true, "1"),
      "user" -> new AtomOperationParamDefine("User", "user", true, "1"),
      "password" -> new AtomOperationParamDefine("Password", "*******", true, "1"),
      "originalSql" -> new AtomOperationParamDefine("Original SQL", "select * from dual", true, "3"),
      "activeSql" -> new AtomOperationParamDefine("Active SQL", "select * from dual", true, "3"),
      "activeUpdateSql" -> new AtomOperationParamDefine("Active Update SQL", "select * from dual", true, "3"))
    val atomOperation = new AtomOperationDefine("AlertEngine", "alertEngine", "AlertEngine.flt", params.toMap)
    atomOperation.id = "alert_engine"
    return atomOperation
  }
}