package dps.atomic.impl

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.sql.{ Row, RowFactory, SparkSession }
import scala.collection.mutable.Map
import org.apache.spark.SparkConf

class RDDString2Dataset(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val viewName = params.get("viewName").get
    val rdd = this.pendingData.asInstanceOf[RDD[String]]
    val result = rdd.map(line => {
      buildRow(line)
    })
    var schema = StructType(specifyTableFields())
    val dataframe = sparkSession.sqlContext.createDataFrame(result, schema)
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
      "buildTableFieldCode" -> new AtomOperationParamDefine(
        "Build Table Field Code",
        """
    /**
     * 定义内存表的字段
     * 数据类型支持:string,int,integer,long,float,double,date,time
     */
    List(
      fieldBuild("field1", "string", false),  //第一列定义,字段名field1,类型为string,不可为空
      fieldBuild("field2", "int", true),      //第二列定义,字段名field2,类型为int,可为空
      fieldBuild("field3", "long")            //第三列定义,字段名field3,类型为long,可为空
    )""", true, "3"),
      "buildTableRowCode" -> new AtomOperationParamDefine(
        "Build Table Field Code",
        """
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