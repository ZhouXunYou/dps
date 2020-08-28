package dps.atomic.impl

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

class OriginalAlarmEngine(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val emmcData = variables.get("EMMC_ALERT_OC_EXTENDS").asInstanceOf[Dataset[Row]]
    val ruleIdentifactions = variables.get("ruleIdentifactions").asInstanceOf[Dataset[Row]]
    
    val group = ruleIdentifactions.rdd.groupBy(row=>{
      row.getAs("alarm_rule_id").asInstanceOf[String]
    })
    
    val rdd = group.map(f=>{
      
      val map = Map[String,String]()
      map.put("alarm_rule_id", f._1)
      f._2.foreach(row=>{
        map.put(row.getAs("identification_field").asInstanceOf[String], row.getAs("expression").asInstanceOf[String])
      })
      map
    })
    emmcData.rdd.filter(row=>{
      var flag:Boolean = false
      rdd.foreach(map=>{
        val alarm_rule_id = map.get("alarm_rule_id")
        map.remove("alarm_rule_id")
        map.keysIterator.foreach(key=>{
          if(!row.getAs(key).asInstanceOf[String].equals(map.get(key))){
            flag = true
            return
          }
        })
      })
      flag
    })
  }
}