package dps.atomic.impl

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import org.apache.spark.ContextCleaner

class RDDKafka2String(override val sparkContext: SparkContext, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val kafkTuple = this.pendingData.asInstanceOf[RDD[Tuple3[String,Int,String]]]
    val groupTopic = kafkTuple.groupBy(tuple=>{
      tuple._1
    })
    val iterableRDD = groupTopic.map(topicLines=>{
      val a:Iterable[String] = null
      val topicName = topicLines._1
      return topicName
//      val topicRDD = sparkContext.parallelize(topicLines._2.toSeq)
    })
    iterableRDD.foreach(i=>{
      println(i)
    })
//    val stringRdd = topicRDD.map(topicTuple=>{
//        topicTuple._3
//      })
//      variables.put(outputVariableKey+"_"+topicName, stringRdd)
  }
  override def define: AtomOperationDefine = {
    val params = Map(
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1")
    )
    val  atomOperation = new AtomOperationDefine("Kafka RDD Handle","kafkaRddHandle","RDDKafka2String.flt",params.toMap)
    atomOperation.id = "rdd_kafka_2_string"
    return atomOperation
  }
}