package dps.atomic.temp.alarm

import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.ArrayList
import dps.atomic.define.AtomOperationDefine
import dps.atomic.impl.AbstractAction

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
    var i:Integer = 0;
    emmcData.rdd.filter(row=>{
      val flags = new ArrayList[Integer]()
      rdd.foreach(map=>{
        
        val alarm_rule_id = map.get("alarm_rule_id")
        map.remove("alarm_rule_id")
        map.keysIterator.foreach(key=>{
          if(!row.getAs(key).asInstanceOf[String].equals(map.get(key))){
            flags.add(1)
          }else{
            flags.add(0)
          }
        })
      })
      flags.contains(1)
    })
  }
  
  override def define: AtomOperationDefine = {
    val params = Map()
    val atomOperation = new AtomOperationDefine("OriginalAlarmEngine", "originalAlarmEngine", "OriginalAlarmEngine.flt", params.toMap,classOf[Nothing],classOf[Nothing],classOf[Nothing],classOf[Nothing])
    atomOperation.id = "original_alarm_engine"
    return atomOperation
  }
}