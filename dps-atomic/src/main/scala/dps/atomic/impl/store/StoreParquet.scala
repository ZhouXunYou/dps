package dps.atomic.impl.utils

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction

class StoreParquet(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

    def doIt(params: Map[String, String]): Any = {
        val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
        if (dataset != null && dataset.isEmpty) {
            return
        }
        val partitionNum = params.get("partitionNum").getOrElse(String.valueOf(sparkSession.sparkContext.defaultMinPartitions)).toInt
        val path = params.get("storePath").get
        val partitionFields = params.getOrElse("partitionFields", "")
        val saveMode: SaveMode = SaveMode.valueOf(params.get("saveMode").getOrElse("Append"))
        if (partitionFields!=null && "".equals(partitionFields)) {
            dataset.repartition(partitionNum).write.mode(saveMode).partitionBy(partitionFields.split(","):_*).parquet(path)
        } else {
            dataset.repartition(partitionNum).write.mode(saveMode).parquet(path)
        }
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "storePath" -> new AtomOperationParamDefine("store.path", "hdfs://${host}:${port}/${warehouse}", true, stringType),
            "saveMode" -> new AtomOperationParamDefine("save.mode", "Append,Overwrite", false, listType),
            "partitionNum" -> new AtomOperationParamDefine("store.path", "hdfs://${host}:${port}/${warehouse}", true, integerType),
            "partitionFields" -> new AtomOperationParamDefine("partition.fields", "filed1,field2", false, stringType))
        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, s"store/${getClassSimpleName}.ftl", params.toMap, classOf[Dataset[_]], classOf[Nothing], classOf[Row], classOf[Nothing])
        return atomOperation
    }
}