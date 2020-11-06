package dps.atomic.impl.alarm

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import dps.atomic.define.AtomOperationDefine
import org.apache.spark.rdd.RDD
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class AlarmEngine4Original(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val rules: Dataset[Row] = this.pendingData.asInstanceOf[Dataset[Row]]

    if (rules.isEmpty) {
      println("+------------------------------+")
      println("无规则数据,跳过原始告警计算")
      println("+------------------------------+")
    } else {

      val original: RDD[Dataset[Row]] = rules.rdd.map(m => {
        val sql = s"""select uuid() as id,
                                  '${m.getAs("alarm_content_expression").asInstanceOf[String]}' as alarm_content,
                                  ${m.getAs("alarm_rule_level").asInstanceOf[Integer]} as alarm_level,
                                  '' as identification_field,wtime as occur_time,
                                  '${m.getAs("alarm_rule_id").asInstanceOf[String]}' as alarm_rule_id 
                                  from alertLBSExtends where ${m.getAs("conditions").asInstanceOf[String]}""".stripMargin

        sparkSession.sqlContext.sql(sql)
      })
      
      this.variables.put(outputVariableKey, original)
    }
  }

  override def define: AtomOperationDefine = {
    val params = Map("sql" -> new AtomOperationParamDefine("Query SQL", "select * from dual", true, "3"))
    val atomOperation = new AtomOperationDefine("AlarmEngine4Original", "alarmEngine4Original", "AlarmEngine4Original.flt", params.toMap)
    atomOperation.id = "alarm_engine_4_original"
    return atomOperation
  }
}