package dps.atomic.temp

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.impl.AbstractAction
import org.apache.spark.sql.Dataset
import dps.atomic.define.AtomOperationUdf
import dps.atomic.define.AtomOperationHasUdfDefine

class StringRDD2Dataset(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
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
        def values = string2Array(line, ",")

        return RowFactory.create(
            values(0), //第一列数据
            java.lang.Integer.valueOf(values.apply(1)), //第二列数据
            java.lang.Long.valueOf(values.apply(2)) //第三列数据
        )
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "viewName" -> new AtomOperationParamDefine("abstract.view.name", "View Name", true, stringType),
            "buildTableFieldCode" -> new AtomOperationParamDefine(
                "build.table.field.code",
                """
    /**
     * 定义内存表的字段
     * 数据类型支持:string,int,integer,long,float,double,date,time
     */
    List(
      fieldBuild("field1", "string", false),  //第一列定义,字段名field1,类型为string,不可为空
      fieldBuild("field2", "int", true),      //第二列定义,字段名field2,类型为int,可为空
      fieldBuild("field3", "long")            //第三列定义,字段名field3,类型为long,可为空
    )""", true, scalaType),
            "buildTableRowCode" -> new AtomOperationParamDefine(
                "build.table.field.code",
                """
    //提供构建内存表的行数据实现
    //字符串分隔
    def values = string2Array(line, ",")
    return RowFactory.create(
      values(0),                                   //第一列数据
      java.lang.Integer.valueOf(values.apply(1)),  //第二列数据
      java.lang.Long.valueOf(values.apply(2))      //第三列数据
    )""", true, scalaType))
        val udfs = Seq(
            new AtomOperationUdf("fieldBuild", Seq(classOf[String].getName, classOf[String].getName, classOf[Boolean].getName)),
            new AtomOperationUdf("fieldBuild", Seq(classOf[String].getName, classOf[String].getName)),
            new AtomOperationUdf("string2Array", Seq(classOf[String].getName, classOf[String].getName)))
        val atomOperation = new AtomOperationHasUdfDefine(getId, getClassName, getClassSimpleName, s"rdd/${getClassSimpleName}.ftl", params.toMap, classOf[RDD[_]], classOf[Dataset[_]], classOf[String], classOf[Row], udfs)
        return atomOperation
    }
}