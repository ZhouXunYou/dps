package dps.datasource

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import dps.datasource.annotations.Params
import dps.datasource.annotations.ParamDefine
import scala.collection.mutable.Map
class JDBCSource(override val sparkContext: SparkContext, override val params: Map[String, String]) extends DataSource(sparkContext,params) {
  val url = params.get("url").get
  val user = params.get("user").get
  val password = params.get("password").get
  val tableName = params.get("tableName").get
  val tableAlias = params.get("tableAlias").getOrElse(null)
  val driver = params.get("driver").get

  override def read(): Any = {

    val dataset = new SQLContext(sparkContext).read.format("jdbc").option("driver", driver).option("url", url).option("dbtable", tableName).option("user", user).option("password", password).load()
    if (this.tableAlias == null) {
      dataset.createOrReplaceTempView(this.tableName)
    } else {
      dataset.createOrReplaceTempView(this.tableAlias)
    }
    dataset
  }
}