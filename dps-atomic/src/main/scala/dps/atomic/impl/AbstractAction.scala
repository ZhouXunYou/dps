package dps.atomic.impl

import data.process.atomic.Action
import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import dps.atomic.define.AtomOperationDefine

abstract class AbstractAction(val sparkContext: SparkContext, val inputVariableKey: String, val outputVariableKey: String, val variables: Map[String, Any]) extends Action with Serializable {
  var pendingData: Any = variables.get(inputVariableKey).getOrElse(null)
  def define():AtomOperationDefine
  var typeMapping = Map(
    "string" -> DataTypes.StringType,
    "int" -> DataTypes.IntegerType,
    "integer" -> DataTypes.IntegerType,
    "long"->DataTypes.LongType,
    "float"->DataTypes.FloatType,
    "double"->DataTypes.DoubleType,
    "date"->DataTypes.DateType,
    "time"->DataTypes.TimestampType
  )
  /**
   * @param fieldName		字段名
   * @param fieldType		字段类型
   * @param nullable		是否可为空
   */
  def fieldBuild(fieldName: String, fieldType: String,nullable:Boolean): StructField = {
    DataTypes.createStructField(fieldName, typeMapping.get(fieldType).get, nullable)
  }
  /**
   * @param fieldName		字段名
   * @param fieldType		字段类型
   */
  def fieldBuild(fieldName: String, fieldType: String): StructField = {
    DataTypes.createStructField(fieldName, typeMapping.get(fieldType).get, true)
  }
  def string2Array(value:String,separator:String):Array[String]={
    value.split(separator)
  }
  def array2String(array:Array[String],separator:String):String={
    array.mkString(separator)
  }
}