package dps.atomic.impl

import data.process.atomic.Action
import scala.collection.mutable.Map
import org.apache.spark.SparkContext

abstract class AbstractAction(val sparkContext:SparkContext,val inputVariableKey: String, val outputVariableKey: String, val variables: Map[String, Any]) extends Action with Serializable {
  var pendingData: Any = variables.get(inputVariableKey).get
}