package ${packagePath}

import dps.mission.CompleteAction
import dps.atomic.model.Mission
import dps.utils.SessionOperation
import dps.atomic.model.OperationParam
import scala.collection.mutable.Map

class ${missionCode}CompleteAction extends CompleteAction {
<#if finishedCode??>
  override def finished(mission:Mission,runParams:Map[String,String]){
    ${finishedCode}
  }
</#if>
}