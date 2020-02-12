package dps.atomic.model

import scala.collection.mutable.Map

class Datasource {
  var datasourceName:String = _
  var implementClass:String = _
  var params:Map[String,String] = _
}