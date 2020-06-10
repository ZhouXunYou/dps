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

class RDDKafka2Dataset(override val sparkContext: SparkContext, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val kafkTuple = variables.get(inputVariableKey).get.asInstanceOf[RDD[Tuple3[String,Int,String]]]
    kafkTuple.groupBy(tuple=>{
      tuple._1
    }).map(topicLines=>{
      val topicName = topicLines._1
      
      val topicRDD = sparkContext.parallelize(topicLines._2.toSeq)
      val stringRdd = topicRDD.map(topicTuple=>{
        topicTuple._3
      })
      variables.put(outputVariableKey+"_"+topicName, stringRdd)
//      topicLines._2.
    })
    
    
  }

  
  override def define: AtomOperationDefine = {
    null
  }

}