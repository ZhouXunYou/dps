package dps.datasource.util

import java.io.File

import scala.collection.immutable.List

import dps.datasource.DataSource
import dps.datasource.FileSource
import java.util.UUID
import dps.utils.JsonUtils
import data.process.util.SessionOperation
import org.apache.spark.SparkContext
import dps.datasource.define.DatasourceDefine
import scala.collection.mutable.Map

object DatasourceUtils {
  private val packageName = "dps.datasource"
  def main(args: Array[String]): Unit = {
//    universe.typeof
    getDatasources().foreach(datasource=>{
//      println(datasource.getName)
      val datasourceInstance = datasource.getConstructor(classOf[SparkContext],classOf[Map[String,String]]).newInstance(null,Map[String,String]()).asInstanceOf[DataSource]
      initDatasource(datasourceInstance)
//      val datasourceDefine = .defin()
//      println(JsonUtils.output(datasourceDefin))
//      val d = typeOf[datasource]
//      datasource.getAnnotation(Params.)
//      datasource.getAnnotations.foreach(annotation=>{
//        println(annotation.annotationType().getName)
//      })
//      println(datasource.getName)
    })
//    val a = Map("a"->"b")
//    
//    
//    new FileSource
  }
  def initDatasource(datasource:DataSource){
    val define = datasource.define()
    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://39.98.141.108:16632/dps", "postgres", "1qaz#EDC")
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
//  def getDatasourceParams(datasource: Class[_]): Array[ParamDefine] = {
//    Array()
//  }
}