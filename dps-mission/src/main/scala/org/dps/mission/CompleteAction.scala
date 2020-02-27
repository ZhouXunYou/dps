package org.dps.mission

import dps.atomic.model.Mission
import dps.utils.SessionOperation
import dps.atomic.model.OperationParam


abstract class CompleteAction {
  def finished(mission: Mission, sessionOperation: SessionOperation) {
    import java.text.SimpleDateFormat
    import java.util.Calendar
    val startTime = getOperationParam(mission,"startTime")
    val interval = getOperationParam(mission,"interval")
    val timeunit = getOperationParam(mission,"timeunit")
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val value = sdf.parse(startTime.operationParamValue)
    val calendar = Calendar.getInstance
    calendar.setTime(value)
    calendar.add(Calendar.HOUR_OF_DAY, Math.abs(Integer.valueOf(interval.operationParamValue)))
    sessionOperation.executeUpdate("update b_mission_operation_param set operation_param_value = ? where id = ?", Array(sdf.format(calendar.getTime),startTime.id))
    println("mission finished")
  }
  def getOperationParam(mission: Mission, operationParamCode: String): OperationParam = {
    mission.operationGroups.foreach(operationGroup => {
      operationGroup.operations.foreach(operation => {
        operation.operationParams.foreach(operationParam => {
          if (operationParam._1.equals(operationParamCode)) {
            return operationParam._2
          }
        })
      })
    })
    return null;
  }
}