package org.dps.mission

import java.util.Optional

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import dps.utils.RunParam
import dps.utils.SessionOperation
import dps.atomic.impl.AbstractAction
import dps.datasource.DataSource
import dps.generator.MissionLoader

object Launcher {
  def main(args: Array[String]): Unit = {
    val params = RunParam.parserArgements(args)
    val requiredKeys = Seq("--missionName","--driver","--url","--user","--password")
    if(!RunParam.validRequiredArgements(params, requiredKeys)){
      println(s"These params is required ${requiredKeys.mkString(",")}. Params format e.g: ${requiredKeys.mkString(" {value},")} {value}")
      println(s"System exit. Please check and try again")
      System.exit(1)
    }
    val missionCode = params.get("--missionName").get
    val so = new SessionOperation(params.get("--driver").get, params.get("--url").get, params.get("--user").get, params.get("--password").get)
    val ml = new MissionLoader(so)
    val mission = ml.getMission(missionCode)
    
    val sparkConf = new SparkConf()
    
    mission.missionParams.foreach(missionParam=>{
      sparkConf.set(missionParam.paramName, Optional.ofNullable(missionParam.paramValue).orElse(missionParam.defaultValue))
    })
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("test")
    val sparkContext = new SparkContext(sparkConf)
    val missionVariables = Map[String,Any]()
    mission.datasources.foreach(datasource=>{
      
      val datasourceInstanceParams = Map[String,String]()
      datasource.params.foreach(param=>{
        datasourceInstanceParams.put(param._1, param._2.paramValue)
      })
      val datasourceInstance = Class.forName(datasource.implementClass)
        .getConstructor(classOf[SparkContext],classOf[Map[String,String]])
        .newInstance(sparkContext,datasourceInstanceParams)
        .asInstanceOf[DataSource]
      missionVariables.put(datasource.datasourceVariableKey, datasourceInstance.read())
    })
    mission.operationGroups.foreach(operationGroup=>{
      operationGroup.operations.foreach(operation=>{
        val actionInstance = Class.forName(operation.classQualifiedName)
          .getConstructor(classOf[SparkContext],classOf[String],classOf[String],classOf[Map[String,Any]])
          .newInstance(sparkContext,operation.inVariableKey,operation.outVariableKey,missionVariables)
          .asInstanceOf[AbstractAction]
        val operationParams = Map[String,String]()
        operation.operationParams.foreach(operationParam=>{
          val paramName = operationParam._1
          val param = operationParam._2
          operationParams.put(paramName, Optional.ofNullable(param.operationParamValue).orElse(param.operationParamDefaultValue))
        })
        actionInstance.doIt(operationParams)
      })
    })
    val completeAction =  Class.forName(s"${mission.missionCode}CompleteAction").newInstance().asInstanceOf[CompleteAction]
    completeAction.finished(mission,so)
  }
}