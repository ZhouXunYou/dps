package dps.datasource

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.Map
import dps.datasource.define.DatasourceDefine
import dps.datasource.define.DatasourceParamDefine

class JDBCSource(override val sparkContext: SparkContext, override val params: Map[String, String]) extends DataSource(sparkContext, params) {
  override def read(): Any = {
    val url = params.get("url").get
    val user = params.get("user").get
    val password = params.get("password").get
    val tableName = params.get("tableName").get
    val tableAlias = params.get("tableAlias").getOrElse(null)
    val driver = params.get("driver").get
    val dataset = new SQLContext(sparkContext).read.format("jdbc").option("driver", driver).option("url", url).option("dbtable", tableName).option("user", user).option("password", password).load()
    if (tableAlias == null) {
      dataset.createOrReplaceTempView(tableName)
    } else {
      dataset.createOrReplaceTempView(tableAlias)
    }
    dataset
  }

  def define(): DatasourceDefine = {
    val paramDefines = Map[String, DatasourceParamDefine](
      "driver" -> new DatasourceParamDefine("JDBC Driver", "org.postgresql.Driver"),
      "url" -> new DatasourceParamDefine("JDBC URL", "jdbc:postgresql://ip:port/database"),
      "user" -> new DatasourceParamDefine("User", "user"),
      "password" -> new DatasourceParamDefine("Password", "******"),
      "tableName" -> new DatasourceParamDefine("Data Table", "table"),
      "tableAlias" -> new DatasourceParamDefine("Data Table Alias", "alias"))
    val datasourceDefine = new DatasourceDefine("JDBC", paramDefines.toMap)
    datasourceDefine.id = "jdbc_source_define"
    return datasourceDefine
  }
}