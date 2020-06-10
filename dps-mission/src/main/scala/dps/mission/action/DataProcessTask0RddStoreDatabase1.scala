package dps.mission.action

import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SaveMode}

import scala.collection.mutable.Map

class DataProcessTask0RddStoreDatabase1(override val sparkContext: SparkContext, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext, inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    dataset.write.format("jdbc")
      .option("driver", params.get("driver").get)
      .option("url", params.get("url").get)
      .option("dbtable", params.get("table").get)
      .option("user", params.get("user").get)
      .option("password", params.get("password").get).mode(SaveMode.Append).save();
  }
}