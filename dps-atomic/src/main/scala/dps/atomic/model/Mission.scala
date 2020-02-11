package dps.atomic.model

import scala.collection.mutable.Map
import scala.collection.immutable.List
class Mission {
  var id: String = _
  var missionName: String = _
  var missionCode: String = _
  var missionType: String = _
  var missionTypeCode: String = _
  var missionParams: Array[MissionParam] = _
  var datasources: List[Datasource] = List()
  var operationGroups: List[OperationGroup] = List()

}