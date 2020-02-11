package dps.datasource

import org.apache.spark.SparkContext
import dps.datasource.annotations.ParamDefine

class FileSource(val sparkContext: SparkContext, override val params: Map[String, String]) extends DataSource(params) {
  override def read(): Any = {
    var partitionNum = sparkContext.defaultMinPartitions
    val paramPartitionNumValue = params.get("partitionNum").get
    if(paramPartitionNumValue!=null && !"".equals(paramPartitionNumValue)){
      partitionNum = paramPartitionNumValue.toInt
    }
    
    sparkContext.textFile(params.get("path").get.asInstanceOf[String],partitionNum)
  }
}