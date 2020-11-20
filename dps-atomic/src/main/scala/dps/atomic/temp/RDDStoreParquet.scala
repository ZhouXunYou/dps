package dps.atomic.temp

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.{ Dataset, Row, SaveMode, SparkSession }
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import com.typesafe.scalalogging.Logger
import dps.atomic.impl.AbstractAction

class RDDStoreParquet(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
  
  val logger = Logger(this.getClass)
  
  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    if (dataset != null && dataset.isEmpty) {
      logger.info("无数据,跳过存储操作")
    } else {
      val partitionNum = params.get("partitionNum").getOrElse(sparkSession.sparkContext.defaultMinPartitions.toString()).toInt
      val modeParam = params.getOrElse("saveMode", "1").toInt
      val path = params.get("path").get
      val repairTables = params.getOrElse("repairTables", "").toString
      val partitionFields = params.getOrElse("partitionFields", "").toString
      var saveMode: SaveMode = SaveMode.Append
      if (params.getOrElse("saveMode", "1").toInt.equals(2)) {
        saveMode = SaveMode.Overwrite
      }

      if (!partitionFields.isEmpty()) {
        var partitions = "";
        val fields = partitionFields.split(",").toList
        for (f <- fields) {
          partitions = partitions.+(f).+(",")
        }
        partitions = partitions.substring(0, partitions.length() - 1)
        dataset.repartition(partitionNum.toInt).write.mode(saveMode).partitionBy(partitions).parquet(path)
      } else {
        dataset.repartition(partitionNum.toInt).write.mode(saveMode).parquet(path)
      }

      if (!repairTables.isEmpty()) {
        repairTables.split(",").foreach(f => {
          sparkSession.sqlContext.sql("MSCK REPAIR TABLE ".+(f));
        })
      }
    }
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "partitionNum" -> new AtomOperationParamDefine("Partition Size", "1", false, "1"),
      "saveMode" -> new AtomOperationParamDefine("Save Mode(1:Append,2:Overwrite)", "1", false, "1"),
      "path" -> new AtomOperationParamDefine("path", "hdfs://${host}:${port}/${warehouse}", true, "1"),
      "repairTables" -> new AtomOperationParamDefine("Repair tables", "Repair hive tables name:db1.table1,db1.table2", false, "1"),
      "partitionFields" -> new AtomOperationParamDefine("Partition fields", "Partition fields:name,age,num", false, "1"))
    val atomOperation = new AtomOperationDefine("RDD Store Parquet", "rddStoreParuet", "RDDStoreParquet.flt", params.toMap,classOf[Nothing],classOf[Nothing],classOf[Nothing],classOf[Nothing])
    atomOperation.id = "rdd_store_parquet"
    return atomOperation
  }
}