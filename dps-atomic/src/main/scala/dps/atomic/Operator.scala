package dps.atomic

import java.util.Optional

import dps.atomic.impl.AbstractAction
import dps.atomic.model.OperationGroup
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

class Operator(val operationGroups: List[OperationGroup], val sparkSession: SparkSession, val sparkConf:SparkConf,val missionVariables: Map[String, Any]) extends Serializable {
  def setVariable(variableKey: String, value: Any) {
    this.missionVariables.put(variableKey, value)
  }

  def operation() {
    operationGroups.foreach(operationGroup => {
      operationGroup.operations.foreach(operation => {
        val actionInstance = Class.forName(operation.classQualifiedName)
          .getConstructor(classOf[SparkSession], classOf[SparkConf], classOf[String], classOf[String], classOf[Map[String, Any]])
          .newInstance(sparkSession, sparkConf,operation.inVariableKey, operation.outVariableKey, missionVariables)
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