package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map

class RDDString2Tuple(override val sparkSession: SparkSession, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends dps.atomic.impl.AbstractAction(sparkSession, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val rdd = this.pendingData.asInstanceOf[RDD[String]]
    val result = rdd.map(line => {
      processStringLine(line)
    })
    this.variables.put(outputVariableKey, result);
  }

  /**
   * @param line : 入参为 Rdd 的每一行数据，类型为 String，即数据集的每一行为一个字符串
   * @return 将字符串变形后的 Tuple 对象
   */
  private def processStringLine(line: String): Tuple2[String, String] = {
    //将字符串按指定的符号分隔
    //e.g: aa,bb,cc,dd -> {aa:bb,cc,dd}
    val array = string2Array(line, ",")
    val newValue = array2String(array.slice(1, array.length), ",")
    val tuple = Tuple2(array(0), newValue)
    return tuple
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "sourceCode" -> new AtomOperationParamDefine("String Process Code",
        """
    //将字符串按指定的符号分隔
    //e.g: aa,bb,cc,dd -> {aa:bb,cc,dd}
    val array = string2Array(line, ",")
    val newValue = array2String(array.slice(1, array.length),",")
    val tuple = Tuple2(array(0),newValue)
    return tuple""", true, "3")
    )

    val atomOperation = new AtomOperationDefine("String RDD to Tuple RDD", "rddString2Tuple", "RDDString2Tuple.flt", params.toMap)
    atomOperation.id = "rdd_string_2_tuple"
    return atomOperation
  }

}