package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

import scala.collection.mutable.Map
import org.apache.spark.SparkConf

class RDDMap2Dataset(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val viewName = params.get("viewName").get
    val rdd = this.pendingData.asInstanceOf[RDD[Map[String, Any]]]
    val result = rdd.map(map => {
      buildRow(map)
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

  private def buildRow(map: Map[String, Any]): Row = {
    //提供构建内存表的行数据实现
    //字符串分隔
    import java.lang.{Long => JavaLong}
    return RowFactory.create(
      map.get("key1").get.asInstanceOf[String], //第一列数据
      map.get("key2").get.asInstanceOf[Integer], //第二列数据
      map.get("key3").get.asInstanceOf[JavaLong] //第三列数据
    )
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "viewName" -> new AtomOperationParamDefine("View Name", "View Name", true, "1"),
      "buildTableFieldCode" -> new AtomOperationParamDefine("Build Table Field Code",
        """
    /**
     * 定义内存表的字段
     */
    List(
      fieldBuild("field1", "string", false),  //第一列定义,字段名field1,类型为string,不可为空
      fieldBuild("field2", "int", true),      //第二列定义,字段名field2,类型为int,可为空
      fieldBuild("field3", "long")            //第三列定义,字段名field3,类型为long,可为空
    )""", true, "3"),
      "buildTableRowCode" -> new AtomOperationParamDefine("Build Table Field Code",
        """
    //提供构建内存表的行数据实现
    import java.lang.{Long => JavaLong}
    return RowFactory.create(
      map.get("key1").get.asInstanceOf[String],    //第一列数据
      map.get("key2").get.asInstanceOf[Integer],   //第二列数据
      map.get("key3").get.asInstanceOf[JavaLong]   //第三列数据
    )""", true, "3"))

    val atomOperation = new AtomOperationDefine("Map RDD to Dataset", "rddMap2Dataset", "RDDMap2Dataset.flt", params.toMap)
    atomOperation.id = "rdd_map_2_dataset"
    return atomOperation
  }

}