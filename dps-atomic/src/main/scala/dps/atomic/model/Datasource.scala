package dps.atomic.model

import scala.collection.mutable.Map

class Datasource {
  var id: String = _
  var datasourceName: String = _
  var implementClass: String = _
  var params: Map[String, DatasourceParam] = _
  var datasourceVariableKey: String = _
}