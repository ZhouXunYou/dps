package dps.atomic.impl
import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class ProcessSQL(override val sparkContext:SparkContext,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext,inputVariableKey, outputVariableKey, variables) with Serializable {
  
  def doIt(params: Map[String, String]): Any = {
    val sql = params.get("sql").get
    val viewName = params.get("viewName").get
    val sqlContext = new SQLContext(sparkContext)
    val dataset = sqlContext.sql(sql);
    dataset.createOrReplaceTempView(viewName);
    this.variables.put(outputVariableKey, dataset);
  }
}