package dps.atomic.model
import scala.collection.immutable.List
import scala.collection.mutable.Map
class Operation {
  var operationName: String = _
  var operationCode: String = _
  var template: String = _
  var classQualifiedName: String = _
  var operationParams: Map[String,OperationParam] = _
  var inVariableKey:String = _
  var outVariableKey:String = _
}