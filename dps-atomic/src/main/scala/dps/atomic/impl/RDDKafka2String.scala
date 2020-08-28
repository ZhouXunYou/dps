package dps.atomic.impl

import java.util.ArrayList

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import com.alibaba.fastjson.JSON

class RDDKafka2String(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val topicName = params.get("topicName").get
    
    val topicValue = this.pendingData.asInstanceOf[RDD[Tuple3[String, Int, String]]].filter(tuple=>{
      tuple._1.equals(topicName)
    })
    val rdd = topicValue.map(tuple=>{
      val t = JSON.parseObject(tuple._3)
      t.put("topicName", topicName)
      t.toJSONString()
    })
    variables.put(outputVariableKey, rdd)
    var topicNames:ArrayList[String] = variables.get("topicNames").getOrElse(null).asInstanceOf[ArrayList[String]]
    if(topicNames==null){
      topicNames = new ArrayList[String]
      
    }
    topicNames.add(topicName)
    variables.put("topicNames", topicNames)
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "topicName" -> new AtomOperationParamDefine("Topic Name", "Topic Name", true, "1"))
    val atomOperation = new AtomOperationDefine("Kafka RDD Handle", "kafkaRddHandle", "RDDKafka2String.flt", params.toMap)
    atomOperation.id = "rdd_kafka_2_string"
    return atomOperation
  }
}