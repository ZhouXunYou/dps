package dps.mission

import dps.atomic.model.Mission
import dps.atomic.model.OperationParam
import scala.collection.mutable.Map

abstract class CompleteAction {
  def finished(mission: Mission,runParam:Map[String,String]) {
          
  }
}