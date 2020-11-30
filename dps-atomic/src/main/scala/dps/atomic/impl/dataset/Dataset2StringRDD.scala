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
import org.apache.spark.rdd.RDD

class Dataset2StringRDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

    override def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        val separator = params.getOrElse("separator", ",")
        dataset.rdd.map(row=>{
            row.toSeq.mkString(separator)
        })
    }
    
    override def define: AtomOperationDefine = {
        val params = Map(
            "separator" -> new AtomOperationParamDefine("value.separator", ",", false, stringType))
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, s"dataset/${getClassSimpleName}.ftl", params.toMap, classOf[Dataset[_]], classOf[RDD[_]], classOf[Row], classOf[String])
        return atomOperation
    }
}