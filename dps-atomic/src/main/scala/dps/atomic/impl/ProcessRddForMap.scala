package dps.atomic.impl
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class ProcessRddForMap(override val sparkContext:SparkContext,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val rdd = this.pendingData.asInstanceOf[RDD[Map[String,Any]]]
    val result = rdd.map(line => {
      processMapLine(line)
    })
    this.variables.put(outputVariableKey, result);
  }
  private def processMapLine(line: Map[String,Any]): String = {
    //将map中的key存放至array中
    val array = Array(line.get("key1"),line.get("key2"))
    //将array中的元素以逗号来进行连接
    array.mkString(",")
    
  }
}