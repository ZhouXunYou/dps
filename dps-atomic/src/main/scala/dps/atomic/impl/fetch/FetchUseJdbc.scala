package dps.atomic.impl.fetch

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.{DAYS, HOURS, MINUTES, SECONDS}
import java.util.{Calendar, Date}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction
import dps.atomic.define.AtomOperationUdf
import dps.atomic.define.AtomOperationHasUdfDefine

class FetchUseJdbc(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val startTime = getTime(params.get("startTime").get)
    val interval = Integer.valueOf(params.get("interval").getOrElse("0"))
    val timeunit = TimeUnit.valueOf(params.get("timeunit").getOrElse("HOURS"))
    val sql = s"(select * from dual where d>='${getTimeWithHour(startTime)}' and d<'${getTime(getTimeWithHour(startTime), interval, timeunit)}')"
    val om = new ObjectMapper with ScalaObjectMapper
    val dsParams = om.readValue(params.get(params.get("dsParamsKey").get).get,classOf[Map[String,String]])
    val dataset = sparkSession.sqlContext.read.format("jdbc")
      .option("url", dsParams.get("url").get)
      .option("driver", dsParams.get("driver").get)
      .option("dbtable", sql)
      .option("user", dsParams.get("user").get)
      .option("password", dsParams.get("password").get)
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
    //    TimeUnit.
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

  override def define: AtomOperationDefine = {
    val params = Map(
      "dsParamsKey" -> new AtomOperationParamDefine("ds.params.key", "dsKey", true, stringType),//JDBC
      "sql" -> new AtomOperationParamDefine("sql", "select * from dual", true, sqlType),
      "viewName" -> new AtomOperationParamDefine("view.name", "dual", true, stringType),
      "startTime" -> new AtomOperationParamDefine("start.time", "yyyy-MM-dd HH:mm:ss", false, stringType),
      "interval" -> new AtomOperationParamDefine("interval", "1", false, integerType),
      "timeunit" -> new AtomOperationParamDefine("time.unit", ",DAYS,HOURS,MINUTES,SECONDS", false, listType))
    val udfs = Seq(new AtomOperationUdf("getTime",Seq(classOf[String].getName)),
            new AtomOperationUdf("getTime",Seq(classOf[String].getName,classOf[Int].getName,classOf[TimeUnit].getName)),
            new AtomOperationUdf("getTimeWithHour",Seq(classOf[String].getName))
            )
    val atomOperation = new AtomOperationHasUdfDefine("fetch.use.jdbc", "fetchUseJdbc", "fetch/FetchUseJdbc.ftl", params.toMap,udfs)
    atomOperation.id = "fetch_use_jdbc"
    return atomOperation
  }

}