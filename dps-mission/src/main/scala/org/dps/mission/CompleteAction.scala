package org.dps.mission

import dps.atomic.model.Mission
import dps.utils.SessionOperation

abstract class CompleteAction {
  def finished(mission:Mission,sessionOperation:SessionOperation){
    import java.lang.{Long => JavaLong}
    val a :JavaLong = 1
  }
}