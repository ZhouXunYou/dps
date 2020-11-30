package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

	override def doIt(params: Map[String, String]): Any = {
        val topicValue = this.pendingData.asInstanceOf[RDD[Tuple3[String, Int, String]]].filter(tuple => {
            tuple._1.equals(inputVariableKey)
        })
        val rdd = topicValue.map(tuple => {
            tuple._3
        })
        variables.put(outputVariableKey, rdd)
    }
}