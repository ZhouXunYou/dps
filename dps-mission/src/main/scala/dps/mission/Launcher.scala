package dps.mission

import java.util.Optional

import scala.collection.Seq
import scala.collection.mutable.Map

import org.apache.spark.sql.SparkSession

import dps.atomic.Operator
import dps.datasource.DataSource
import dps.datasource.StreamDatasource
import dps.generator.MissionLoader
import dps.utils.RunParam
import dps.utils.SessionOperation
import org.apache.spark.SparkConf

object Launcher {
  def main(args: Array[String]): Unit = {
    val params = RunParam.parserArgements(args)
    val requiredKeys = Seq("--missionName", "--driver", "--ip", "--port", "--user", "--password", "--dbType", "--dbName")
    if (!RunParam.validRequiredArgements(params, requiredKeys)) {
      println(s"These params is required ${requiredKeys.mkString(",")}. Params format e.g: ${requiredKeys.mkString(" {value},")} {value}")
      println(s"System exit. Please check and try again")
      System.exit(1)
    }
    val missionCode = params.get("--missionName").get
    val so = new SessionOperation(params.get("--driver").get, params.get("--ip").get, params.get("--port").get, params.get("--user").get, params.get("--password").get, params.get("--dbType").get, params.get("--dbName").get)
    val ml = new MissionLoader(so)
    val mission = ml.getMission(missionCode)
    /** 验证任务是否执行，定制开发 **/
    // TODO 后续删除这部分
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    mission.operationGroups.foreach(og => {
//      og.operations.foreach(operation => {
//        operation.operationParams.foreach(param => {
//          if (param._1.equals("startTime")) {
//            val startTime = param._2.operationParamValue
//            val startDateTime = sdf.parse(startTime)
//            val current = Calendar.getInstance().getTime
//            if (startDateTime.getTime >= current.getTime) {
//              println(s"Start time is exception, start time is ${startDateTime}, current time is ${current}. Mission abort")
//              System.exit(0)
//            }
//          }
//        })
//      })
//    })
    /** 验证任务是否执行，定制开发 **/
    val builder = SparkSession.builder()
    val sparkConf = new SparkConf
    mission.missionParams.foreach(missionParam => {
//      builder.config(missionParam.paramName, Optional.ofNullable(missionParam.paramValue).orElse(missionParam.defaultValue))
      sparkConf.set(missionParam.paramName, Optional.ofNullable(missionParam.paramValue).orElse(missionParam.defaultValue))
    })
    builder.config(sparkConf)
    builder.appName(mission.missionCode)
    val sparkSession = builder.enableHiveSupport().getOrCreate()
    val missionVariables = Map[String, Any]()

    val datasourceInstanceParams = Map[String, String]()
    mission.datasource.params.foreach(param => {
      datasourceInstanceParams.put(param._1, param._2.paramValue)
    })
    
    val operator = new Operator(mission.operationGroups,sparkSession,sparkConf,missionVariables)
    
    val datasourceInstance = Class.forName(mission.datasource.implementClass)
      .getConstructor(classOf[SparkSession], classOf[SparkConf], classOf[Map[String, String]],classOf[Operator])
      .newInstance(sparkSession, sparkConf, datasourceInstanceParams,operator)
      .asInstanceOf[DataSource]
    datasourceInstance.read(mission.datasource.datasourceVariableKey);
    if (datasourceInstance.isInstanceOf[StreamDatasource]) {
      datasourceInstance.asInstanceOf[StreamDatasource].start()
    } else {
      val completeAction = Class.forName(s"dps.mission.action.${mission.missionCode}CompleteAction").newInstance().asInstanceOf[CompleteAction]
      completeAction.finished(mission, params)
    }
  }
}