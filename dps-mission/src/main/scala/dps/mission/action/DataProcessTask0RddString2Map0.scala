package dps.mission.action
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map

class DataProcessTask0RddString2Map0(override val sparkContext:SparkContext,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val rdd = this.pendingData.asInstanceOf[RDD[String]]
    val result = rdd.map(line => {
      processStringLine(line)
    })
    this.variables.put(outputVariableKey, result);
  }
  /**
   * @param line: 入参为 Rdd 的每一行数据，类型为 String
   * @return 将字符串变形后的 Map 对象
   */
  private def processStringLine(line: String): Map[String, Any] = {
    	println("=========>>>>>>")
	println(line)
	println("=========<<<<<<")
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