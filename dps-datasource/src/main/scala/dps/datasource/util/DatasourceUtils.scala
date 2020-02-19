package dps.datasource.util

import java.io.File

import scala.collection.mutable.Map

import org.apache.spark.SparkContext

import data.process.util.SessionOperation
import dps.datasource.DataSource

object DatasourceUtils {
  private val packageName = "dps.datasource"
  def main(args: Array[String]): Unit = {
    //    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://39.98.141.108:16632/dps", "postgres", "1qaz#EDC")
//    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://10.1.1.99:5432/dps", "postgres", "postgres")
    val so = new SessionOperation("com.mysql.jdbc.Driver", "jdbc:mysql://39.98.141.108:16606/dps?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "1qaz#EDC")
    so.executeUpdate("truncate table s_datasource_param_define", Array())
    so.executeUpdate("truncate table s_datasource_define", Array())
    getDatasources().foreach(datasource=>{
      val datasourceInstance = datasource.getConstructor(classOf[SparkContext],classOf[Map[String,String]]).newInstance(null,Map[String,String]()).asInstanceOf[DataSource]
      initDatasource(datasourceInstance,so)
    })
  }
  def initDatasource(datasource:DataSource,so:SessionOperation){
    val define = datasource.define()
    val datasourceParams = Array[Any](define.id,define.datasourceName,datasource.getClass.getName)
    so.executeUpdate("insert into s_datasource_define(id,datasource_name,datasource_class) values (?,?,?)", datasourceParams)
    define.datasourceDefinParams.foreach(datasourceDefinParam=>{
      val datasourceParamCode = datasourceDefinParam._1
      val datasourceParamDefine = datasourceDefinParam._2
      val id = s"${define.id}_${datasourceParamCode}"
      val params = Array[Any](id,define.id,datasourceParamCode,datasourceParamDefine.paramName,datasourceParamDefine.defaultValue)
      so.executeUpdate("insert into s_datasource_param_define(id,datasource_id,datasource_param_code,datasource_param_name,datasource_param_default_value) values(?,?,?,?,?)", params)
    })
  }
  def getDatasources(): Array[Class[_]] = {
    var datasources = List[Class[_]]()
    val url = DatasourceUtils.getClass.getClassLoader.getResource(packageName.replace(".", "/"))
    val sourcePackage = new File(url.getFile)
    sourcePackage.listFiles().foreach(file=>{
      if(file.isFile() && file.getName.endsWith(".class")){
        val clazz = Class.forName(Array(packageName,file.getName.split("\\.").apply(0)).mkString("."))
        if(clazz.getSuperclass == classOf[DataSource]){
          datasources = clazz :: datasources
        }
      }
    })
    datasources.reverse.toArray
  }
}