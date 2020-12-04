package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    override def doIt(params: Map[String, String]): Any = {
        val leftRDD = params.get("left").get.asInstanceOf[RDD[Tuple2[String, String]]]
        val rightRDD = params.get("right").get.asInstanceOf[RDD[Tuple2[String, String]]]
        val joinRDD = leftRDD.join(rightRDD)
        val newRDD = joinRDD.map(tuple => {
            tuple._2._1 ++ tuple._2._2
        })
        return newRDD
    }
}