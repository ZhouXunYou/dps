package dps.atomic.impl.alarm

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import dps.atomic.define.AtomOperationDefine
import dps.atomic.impl.AbstractAction

class ActivityAlarmEngine(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    var alarmRules = variables.get("alarmRules").asInstanceOf[Dataset[Row]]
    var baseRuleAlarms = variables.get("baseRuleAlarms").asInstanceOf[Dataset[Row]]
    alarmRules.rdd.map(row=>{
      (row.getAs("id").asInstanceOf[String],row.getAs("occur_count").asInstanceOf[Integer],row.getAs("open_upgrade").asInstanceOf[Integer])
    })
  }
  
  override def define: AtomOperationDefine = {
    val params = Map()
    val atomOperation = new AtomOperationDefine("ActivityAlarmEngine", "activityAlarmEngine", "ActivityAlarmEngine.flt", params.toMap)
    atomOperation.id = "activity_alarm_engine"
    return atomOperation
  }
}