package dps.atomic.impl.dataset

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders

class Dataset2MapRDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

    override def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        dataset.rdd.map(row => {
            rowTransformMap(row)
        })
    }

    private def rowTransformMap(row: Row): Map[String, Any] = {
        Map(
            "field1" -> row.get(0),
            "field2" -> row.get(1))
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "row2MapCode" -> new AtomOperationParamDefine("dataset.row.transform.map", """
        Map(
            "field1" -> row.get(0),
            "field2" -> row.get(1))""", true, scalaType))
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, s"dataset/${getClassSimpleName}.ftl", params.toMap, classOf[Nothing], classOf[Dataset[_]], classOf[Nothing], classOf[Row])
        return atomOperation
    }
}