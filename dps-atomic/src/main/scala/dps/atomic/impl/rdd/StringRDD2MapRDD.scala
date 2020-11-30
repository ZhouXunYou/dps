package dps.atomic.impl.rdd

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction
import dps.atomic.define.AtomOperationUdf
import dps.atomic.define.AtomOperationHasUdfDefine

class StringRDD2MapRDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
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
        //将字符串按指定的符号分隔
        val array = string2Array(line, ",")
        //初始化Map对象
        val map = Map[String, Any]()
        //为map对象赋值
        map.put("key0", array.apply(0))
        map.put("key1", array.apply(1))
        //返回
        return map
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "stringProcessCode" -> new AtomOperationParamDefine(
                "string.process.code",
                """
    //将字符串按指定的符号分隔
    val array = string2Array(line, ",")
    //初始化Map对象
    val map = Map[String, Any]()
    //为map对象赋值
    map.put("key0", array.apply(0))
    map.put("key1", array.apply(1))
    //返回
    return map""", true, scalaType))
        val udfs = Seq(
            new AtomOperationUdf("string2Array", Seq(classOf[String].getName, classOf[String].getName)))
        val atomOperation = new AtomOperationHasUdfDefine(getId, getClassName, getClassSimpleName, s"rdd/${getClassSimpleName}.ftl", params.toMap, classOf[RDD[_]], classOf[RDD[_]], classOf[String], classOf[Map[String, Any]], udfs)
        return atomOperation
    }
}