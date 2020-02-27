package org.dps.mission

import dps.atomic.model.Mission
import dps.utils.SessionOperation

abstract class CompleteAction {
  def finished(mission:Mission,sessionOperation:SessionOperation){
//    mission.id
//    sessionOperation.executeUpdate(, params)
  }
}