package dps.mission

import java.util
import java.util.Optional

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

object AlarmEngineTest {
  def main(args: Array[String]): Unit = {
    val params: Map[String, String] = Map(
      "url" -> "jdbc:postgresql://192.168.11.200:5432/emmc",
      "driver" -> "org.postgresql.Driver",
      //      "ruleSql" -> "(select sar.id,sar.aggregate_occur_count,sar.alarm_content_expression,sar.alarm_rule_level,sar.alarm_rule_name,sar.occur_count from s_alarm_rule sar inner join s_alarm_rule_relation b on sar.id = b.alarm_rule_id where sar.alarm_rule_status = 1) as alarmRule",
      //      "identificationSql" -> "(select c.alarm_rule_id,c.identification_field,c.expression,c.id from s_alarm_rule a inner join s_alarm_rule_identification c on a.id = c.alarm_rule_id where a.alarm_rule_status = 1) as ruleIdentification",
      //      "conditionSql" -> "(select d.alarm_rule_id,d.condition_field,d.expression,d.comparison,d.and_or,d.id from s_alarm_rule a inner join s_alarm_rule_condition d on a.id = d.alarm_rule_id where a.alarm_rule_status = 1) as ruleCondition",
      "ruleSql" -> "select sar.id,sar.aggregate_occur_count,sar.alarm_content_expression,sar.alarm_rule_level,sar.alarm_rule_name,sar.occur_count from s_alarm_rule sar inner join s_alarm_rule_relation b on sar.id = b.alarm_rule_id where sar.alarm_rule_status = 1",
      "identificationSql" -> "select c.alarm_rule_id,c.identification_field,c.expression,c.id from s_alarm_rule a inner join s_alarm_rule_identification c on a.id = c.alarm_rule_id where a.alarm_rule_status = 1",
      "conditionSql" -> "select d.alarm_rule_id,d.condition_field,d.expression,d.comparison,d.and_or,d.id from s_alarm_rule a inner join s_alarm_rule_condition d on a.id = d.alarm_rule_id where a.alarm_rule_status = 1",
      "user" -> "postgres",
      "password" -> "postgres")

    val builder = SparkSession.builder()
    val sparkConf = new SparkConf
    sparkConf.setAppName("test").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.executor.memory", "8g")
    builder.config(sparkConf)
    val sparkSession = builder.getOrCreate()

    val ruleDataset: Dataset[Row] = jdbcQuery(params, "ruleSql", sparkSession)
    val identificationDataset: Dataset[Row] = jdbcQuery(params, "identificationSql", sparkSession)
    val conditionDataset: Dataset[Row] = jdbcQuery(params, "conditionSql", sparkSession)

    val rddWheres: RDD[Tuple2[String, String]] = conditionsSplicing(identificationDataset, conditionDataset)

    ruleSplicing(ruleDataset, rddWheres)

  }

  /**
   * JDBC查询返回Dataset[Row]
   *
   * @param params
   * @param keyForSQL
   * @return
   */
  private def jdbcQuery(params: Map[String, String], keyForSQL: String, sparkSession: SparkSession): Dataset[Row] = {
    val sql = s"(${params.get(keyForSQL).get}) as tmpView"
    sparkSession.sqlContext.read.format("jdbc")
      .option("url", params.get("url").get)
      .option("driver", params.get("driver").get)
      .option("dbtable", sql)
      .option("user", params.get("user").get)
      .option("password", params.get("password").get)
      .load();
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

  private def ruleSplicing(ruleDataset: Dataset[Row], rddWheres: RDD[Tuple2[String, String]]) = {

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
        "occur_count" -> t._2._1.get(4),
        "conditions" -> t._2._2)
    })
  }

  /**
   * 告警处理
   *
   * @param rules
   * @param params
   * @param sparkSession
   */
  private def alarmHandle(rules: RDD[Map[String, Any]], params: Map[String, String], sparkSession: SparkSession) = {

    //    rules.foreach(m => {
    //      println(m.get("id"))
    //      println(m.get("aggregate_occur_count"))
    //      println(m.get("alarm_content_expression"))
    //      println(m.get("alarm_rule_level"))
    //      println(m.get("alarm_rule_name"))
    //      println(m.get("occur_count"))
    //      println(m.get("conditions"))
    //      println("////////////////////////")
    //    })

    rules.foreach(m => {
      val sql = s"${params.get("alarmSql").get}"
      val dataset = sparkSession.sqlContext.sql(sql);
      dataset.write.format("jdbc")
        .option("driver", params.get("driver").get)
        .option("url", params.get("url").get)
        .option("dbtable", params.get("alarmTable").get)
        .option("user", params.get("user").get)
        .option("password", params.get("password").get).mode(SaveMode.Append).save();
    })
  }
}