package dps.datasource

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine
import dps.datasource.define.DatasourceParamDefine

class FileSource(override val sparkContext: SparkContext, override val params: Map[String, String]) extends DataSource(sparkContext, params) {
  override def read(): Any = {
    val filePath = params.get("path").get
    var partitionNum = sparkContext.defaultMinPartitions
    val paramPartitionNumValue = params.get("partitionNum").get
    if (paramPartitionNumValue != null && !"".equals(paramPartitionNumValue)) {
      partitionNum = paramPartitionNumValue.toInt
    }
    sparkContext.textFile(filePath, partitionNum)
  }

  def define(): DatasourceDefine = {
    val paramDefines = Map[String, DatasourceParamDefine](
      "path" -> new DatasourceParamDefine("File path", "file:///opt/${fileName} or hdfs://${host}:${port}/${fileName}"))
    val datasourceDefine = new DatasourceDefine("File", paramDefines.toMap)
    datasourceDefine.id = "file_source_define"
    return datasourceDefine
  }
}