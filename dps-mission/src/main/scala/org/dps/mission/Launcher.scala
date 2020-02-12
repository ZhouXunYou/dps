package org.dps.mission

import data.process.util.RunParam
import data.process.util.SessionOperation
import dps.generator.MissionLoader
import org.apache.spark.SparkConf
import java.util.Optional
import org.apache.spark.SparkContext
import dps.datasource.DataSource
import scala.collection.mutable.Map
import dps.atomic.impl.AbstractAction

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
    val sparkContext = new SparkContext(sparkConf)
    val missionVariables = Map[String,Any]()
    mission.datasources.foreach(datasource=>{
      val datasourceInstance = Class.forName(datasource.implementClass)
        .getConstructor(classOf[SparkContext],classOf[Map[String,String]])
        .newInstance(sparkContext,datasource.params)
        .asInstanceOf[DataSource]
      missionVariables.put(datasource.datasourceName, datasourceInstance.read())
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
    
    
  }
}