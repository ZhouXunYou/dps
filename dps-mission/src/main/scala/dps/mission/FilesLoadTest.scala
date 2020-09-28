package dps.mission

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._
import org.apache.spark.sql.SparkSession

object FilesLoadTest {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
    val sparkConf = new SparkConf
    sparkConf.setAppName("test").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.executor.memory", "8g")
    builder.config(sparkConf)
    val sparkSession = builder.getOrCreate()
    
    
    val path = "hdfs://cdhnode209:8020/emmc/humanmigrated"
    val df = sparkSession.sqlContext.read.load(path);
    println(df.count())
    df.printSchema()
    df.show()
  }
}