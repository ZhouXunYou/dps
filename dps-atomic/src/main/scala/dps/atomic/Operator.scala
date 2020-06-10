package dps.atomic

import dps.atomic.model.Mission
import org.apache.spark.SparkContext
import java.util.Optional
import dps.atomic.impl.AbstractAction
import scala.collection.mutable.Map
import dps.atomic.model.OperationGroup

class Operator (val operationGroups:List[OperationGroup],sparkContext:SparkContext,missionVariables:Map[String, Any]) extends Serializable{
  def setVariable(variableKey:String,value:Any){
    this.missionVariables.put(variableKey, value)
  }
  def operation(){
    println(">>>>>>>>>>>>>>>>>>>>>")
    println(operationGroups)
    println("<<<<<<<<<<<<<<<<<<<<<")
    operationGroups.foreach(operationGroup => {
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
  }
}