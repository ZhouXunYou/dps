package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row,Dataset}
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[Map[String, Any]]]
        val viewName = params.get("viewName").get
        import sparkSession.implicits._
        val dataframe = rdd.toDF()
        dataframe.createOrReplaceTempView(viewName)
        this.variables.put(outputVariableKey, dataframe);
    }
}