package dps.mission

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.{ SparkConf, SparkContext }
import org.neo4j.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Connection
import java.util.Properties

object JDBCTest {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
    builder.appName("jdbcTest")
    val sparkConf = new SparkConf
    sparkConf.set("spark.master", "local[*]")
    sparkConf.set("spark.executor.memory", "2g")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    builder.config(sparkConf)
    val sparkSession = builder.getOrCreate()
    //    val sql =
    //      s"""(
    //          select ar.id as alarm_rule_id,
    //                 ar.aggregate_occur_count,
    //                 ar.alarm_content_expression,
    //                 ar.alarm_rule_level,
    //                 ar.alarm_rule_name,
    //                 ar.occur_count,
    //                 ar.alarm_type,
    //                 arr.left_relation,
    //                 ari.expression
    //            from s_alarm_rule ar
    //            inner join s_alarm_rule_relation arr
    //            on ar.id = arr.alarm_rule_id
    //            inner join s_alarm_rule_identification ari
    //            on ar.id = ari.alarm_rule_id
    //            where ar.alarm_rule_status = 1
    //            and ar.alarm_type = 'BASE_STATION_AREA'
    //            and ari.identification_field = 'areaId'
    //          ) as tmpRuleView""".stripMargin
    //    val dataset = sparkSession.sqlContext.read.format("jdbc")
    //      .option("url", "jdbc:postgresql://192.168.11.200:5432/emmc")
    //      .option("driver", "org.postgresql.Driver")
    //      .option("dbtable", sql)
    //      .option("user", "postgres")
    //      .option("password", "postgres")
    //      .load();
    //
    //    dataset.show()
    //    dataset.filter("substring(expression, 7) = '000000'").show()
    //    dataset.filter("substring(expression, 7) <> '000000'").show()
    val params: Map[String, String] = Map(
      "table" -> "b_alarm",
      "field" -> "abc")

    val sql = s""""delete from ${params.get("table").get} where ${params.getOrElse("field", "id")} = ?1""".stripMargin

    println(sql)
    println(params.getOrElse("field", "id"))
    println("\"".+(params.getOrElse("field", "id")).+("\""))
  }
}