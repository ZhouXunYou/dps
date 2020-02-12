package dps.datasource

import org.apache.spark.SparkContext
import scala.collection.mutable.Map

abstract class DataSource(val sparkContext: SparkContext,val params:Map[String,String]) {
   def read(): Any
}