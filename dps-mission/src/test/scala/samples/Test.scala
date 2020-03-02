package samples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("test")
    sparkConf.setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(Data.data)
    val om = new ObjectMapper with ScalaObjectMapper
    
    val rows = rdd.map(content => {
      om.registerModule(DefaultScalaModule)
      om.readValue(content, classOf[STP])
    }).map(stp => {
      RowFactory.create(
        stp.sourceAddress,
        stp.targetAddress,
        stp.sourcePort,
        stp.targetPort,
        stp.transportProto,
        stp.applicationProto,
        stp.packageCount,
        stp.packageSize)
    })
    var schema = StructType(List(
      DataTypes.createStructField("sourceAddress", DataTypes.StringType, true),
      DataTypes.createStructField("targetAddress", DataTypes.StringType, true),
      DataTypes.createStructField("sourcePort", DataTypes.IntegerType, true),
      DataTypes.createStructField("targetPort", DataTypes.IntegerType, true),
      DataTypes.createStructField("transportProto", DataTypes.StringType, true),
      DataTypes.createStructField("applicationProto", DataTypes.StringType, true),
      DataTypes.createStructField("packageCount", DataTypes.IntegerType, true),
      DataTypes.createStructField("packageSize", DataTypes.LongType, true)))
    val dataframe = new SQLContext(sparkContext).createDataFrame(rows, schema)
    dataframe.createOrReplaceTempView("stp")
    val result = new SQLContext(sparkContext).sql("select sourceAddress,targetAddress,sourcePort,targetPort,transportProto,applicationProto,sum(packageCount),sum(packageSize) from stp group by sourceAddress,targetAddress,sourcePort,targetPort,transportProto,applicationProto")
    println("-------------------",result.count())
    result.show(1000)
  }
}