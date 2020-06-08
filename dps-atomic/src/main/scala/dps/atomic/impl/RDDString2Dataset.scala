package dps.atomic.impl

import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine

class RDDMap2Dataset(override val sparkContext: SparkContext, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkContext, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val viewName = params.get("viewName").get
    val rdd = this.pendingData.asInstanceOf[RDD[String]]
    val result = rdd.map(line => {
      buildRow(line)
    })
    var schema = StructType(specifyTableFields())
    val dataframe = new SQLContext(sparkContext).createDataFrame(result, schema)
    dataframe.createOrReplaceTempView(viewName)
    this.variables.put(outputVariableKey, dataframe);
  }

  private def specifyTableFields(): List[StructField] = {
    /**
     * 定义内存表的字段
     */
    List(
      fieldBuild("field1", "string", false), //第一列定义,字段名field1,类型为string,不可为空
      fieldBuild("field2", "int", true), //第二列定义,字段名field2,类型为int,可为空
      fieldBuild("field3", "long") //第三列定义,字段名field3,类型为long,可为空
    )
  }

  private def buildRow(line: String): Row = {
    //提供构建内存表的行数据实现
    //字符串分隔
    def values = line.split(",")
    return RowFactory.create(
      values(0), //第一列数据
      java.lang.Integer.valueOf(values.apply(1)), //第二列数据
      java.lang.Long.valueOf(values.apply(2)) //第三列数据
    )
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1"),
      "buildTableFieldCode" -> new AtomOperationParamDefine("Build Table Field Code", """
    /**
     * 定义内存表的字段
     */
    List(
      fieldBuild("field1", "string", false),  //第一列定义,字段名field1,类型为string,不可为空
      fieldBuild("field2", "int", true),      //第二列定义,字段名field2,类型为int,可为空
      fieldBuild("field3", "long")            //第三列定义,字段名field3,类型为long,可为空
    )""", true, "3"),
      "buildTableRowCode" -> new AtomOperationParamDefine("Build Table Field Code", """
    //提供构建内存表的行数据实现
    //字符串分隔
    def values = line.split(",")
    return RowFactory.create(
      values(0),                                   //第一列数据
      java.lang.Integer.valueOf(values.apply(1)),  //第二列数据
      java.lang.Long.valueOf(values.apply(2))      //第三列数据
    )""", true, "3"))

    val atomOperation = new AtomOperationDefine("String RDD to Dataset", "rddString2Dataset", "RDDString2Dataset.flt", params.toMap)
    atomOperation.id = "rdd_string_2_dataset"
    return atomOperation
  }

}