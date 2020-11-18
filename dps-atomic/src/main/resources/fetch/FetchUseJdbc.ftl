package ${packagePath}

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.{DAYS, HOURS, MINUTES, SECONDS}
import java.util.{Calendar, Date}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction
import scala.collection.mutable.Map
import org.apache.spark.SparkConf

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val startTime = getTime(params.get("startTime").get)
    val interval = Integer.valueOf(params.get("interval").getOrElse("0"))
    val timeunit = TimeUnit.valueOf(params.get("timeunit").getOrElse("HOURS"))
    val sql = s"""(${sql}) as ${className}_view"""
    val dataset = sparkSession.sqlContext.read.format("jdbc")
      .option("url", params.get("url").get)
      .option("driver", params.get("driver").get)
      .option("dbtable", sql)
      .option("user", params.get("user").get)
      .option("password", params.get("password").get)
      .load();
    dataset.createOrReplaceTempView(params.get("viewName").get)
    variables.put(outputVariableKey, dataset)
  }
  def getTime(paramValue: String): String = {
    if (paramValue == null || "".equals(paramValue.trim())) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      return sdf.format(new Date())
    }
    return paramValue;
  }
  def getTime(startTime: String, interval: Int, unit: TimeUnit): String = {
    if (startTime == null || "".equals(startTime)) {
      return getTime(startTime)
    }
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(startTime)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    val endTime = unit match {
      case DAYS => {
        calendar.add(Calendar.DAY_OF_MONTH, interval)
        calendar.getTime
      }
      case HOURS => {
        calendar.add(Calendar.HOUR, interval)
        calendar.getTime
      }
      case MINUTES => {
        calendar.add(Calendar.MINUTE, interval)
        calendar.getTime
      }
      case SECONDS => {
        calendar.add(Calendar.SECOND, interval)
        calendar.getTime
      }
      case _ => {
        calendar.getTime
      }
    }
    sdf.format(endTime)
  }
  def getTimeWithHour(paramValue: String): String = {
    if (paramValue == null || "".equals(paramValue.trim())) {
      val sdf = new SimpleDateFormat()
      sdf.applyPattern("yyyy-MM-dd HH:mm:ss")
      val calendar = Calendar.getInstance
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
      return sdf.format(calendar.getTime)
    }
    return paramValue
  }
}