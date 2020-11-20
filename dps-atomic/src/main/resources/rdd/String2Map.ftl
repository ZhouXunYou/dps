package ${packagePath}

import dps.atomic.impl.AbstractAction
import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import org.apache.spark.SparkConf

class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  
    override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[String]]
        val result = rdd.map(line => {
            processStringLine(line)
        })
        this.variables.put(outputVariableKey, result);
    }

    private def processStringLine(line: String): Map[String, Any] = {
        //将字符串按指定的符号分隔
        val array = line.split(",")
        //初始化Map对象
        val map = Map[String, Any]()
        //为map对象赋值
        map.put("key0", array.apply(0))
        map.put("key1", array.apply(1))
        //返回
        return map
    }
}