package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[String]]
        val result = rdd.map(line => {
            processStringLine(line)
        })
        this.variables.put(outputVariableKey, result);
    }

    /**
     * @param line : 入参为 Rdd 的每一行数据，类型为 String，即数据集的每一行为一个字符串
     * @return 将字符串变形后的 Map 对象
     */
    private def processStringLine(line: String): Map[String, Any] = {
        ${stringProcessCode}
    }
}