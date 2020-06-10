package dps.datasource

import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import dps.atomic.Operator
import dps.datasource.define.DatasourceDefine
import dps.datasource.define.DatasourceParamDefine
import org.apache.spark.sql.SparkSession

class FileSource(override val sparkSession: SparkSession, override val params: Map[String, String],override val operator:Operator) extends DataSource(sparkSession, params,operator) {
  override def read(variableKey:String)= {
    val filePath = params.get("path").get
    var partitionNum = sparkSession.sparkContext.defaultMinPartitions
    val paramPartitionNumValue = params.get("partitionNum").get
    if (paramPartitionNumValue != null && !"".equals(paramPartitionNumValue)) {
      partitionNum = paramPartitionNumValue.toInt
    }
    val rdd = sparkSession.sparkContext.textFile(filePath, partitionNum)
    operator.setVariable(variableKey, rdd)
  }

  def define(): DatasourceDefine = {
    val paramDefines = Map[String, DatasourceParamDefine](
      "path" -> new DatasourceParamDefine("File path", "file:///opt/${fileName} or hdfs://${host}:${port}/${fileName}"))
    val datasourceDefine = new DatasourceDefine("File", paramDefines.toMap)
    datasourceDefine.id = "file_source_define"
    return datasourceDefine
  }
}