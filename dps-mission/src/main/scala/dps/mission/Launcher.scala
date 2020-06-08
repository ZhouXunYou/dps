package dps.mission

import java.util.Optional
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import dps.utils.RunParam
import dps.utils.SessionOperation
import dps.atomic.impl.AbstractAction
import dps.datasource.DataSource
import dps.generator.MissionLoader
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.Seq
import dps.datasource.StreamDatasource

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
    //    mission.operationGroups.foreach(og=>{
    //      og.operations.foreach(operation=>{
    //        operation.operationParams.foreach(param=>{
    //          if(param._1.equals("startTime")){
    //            val startTime = param._2.operationParamValue
    //            val startDateTime = sdf.parse(startTime)
    //            val current = Calendar.getInstance().getTime
    //            if(startDateTime.getTime>=current.getTime){
    //              println(s"Start time is exception, start time is ${startDateTime}, current time is ${current}. Mission abort")
    //              System.exit(0)
    //            }
    //          }
    //        })
    //      })
    //    })
    /** 验证任务是否执行，定制开发 **/
    val sparkConf = new SparkConf()

    mission.missionParams.foreach(missionParam => {
      sparkConf.set(missionParam.paramName, Optional.ofNullable(missionParam.paramValue).orElse(missionParam.defaultValue))
    })
    //    sparkConf.setMaster(mission.missionParams)
    sparkConf.setAppName(mission.missionName)
    val sparkContext = new SparkContext(sparkConf)
    val missionVariables = Map[String, Any]()

    val datasourceInstanceParams = Map[String, String]()
    mission.datasource.params.foreach(param => {
      datasourceInstanceParams.put(param._1, param._2.paramValue)
    })
    val datasourceInstance = Class.forName(mission.datasource.implementClass)
      .getConstructor(classOf[SparkContext], classOf[Map[String, String]])
      .newInstance(sparkContext, datasourceInstanceParams)
      .asInstanceOf[DataSource]
    missionVariables.put(mission.datasource.datasourceVariableKey, datasourceInstance.read())
    mission.operationGroups.foreach(operationGroup => {
      operationGroup.operations.foreach(operation => {
        val actionInstance = Class.forName(operation.classQualifiedName)
          .getConstructor(classOf[SparkContext], classOf[String], classOf[String], classOf[Map[String, Any]])
          .newInstance(sparkContext, operation.inVariableKey, operation.outVariableKey, missionVariables)
          .asInstanceOf[AbstractAction]
        val operationParams = Map[String, String]()
        operation.operationParams.foreach(operationParam => {
          val paramName = operationParam._1
          val param = operationParam._2
          operationParams.put(paramName, Optional.ofNullable(param.operationParamValue).orElse(param.operationParamDefaultValue))
        })
        actionInstance.doIt(operationParams)
      })
    })
    if (datasourceInstance.isInstanceOf[StreamDatasource]) {
      mission.datasource.asInstanceOf[StreamDatasource].start()
    } else {
      val completeAction = Class.forName(s"dps.mission.action.${mission.missionCode}CompleteAction").newInstance().asInstanceOf[CompleteAction]
      completeAction.finished(mission, params)
    }
  }
}