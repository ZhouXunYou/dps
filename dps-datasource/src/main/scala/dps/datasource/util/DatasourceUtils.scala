package dps.datasource.util

import java.io.File

import scala.collection.immutable.List

import dps.datasource.DataSource
import dps.datasource.annotations.ParamDefine
import dps.datasource.annotations.Params


object DatasourceUtils {
  private val packageName = "dps.datasource"
  def main(args: Array[String]): Unit = {
    getDatasources().foreach(datasource=>{
      println(datasource.getName)
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
    println(datasources.length)
    datasources.reverse.toArray
  }
  def getDatasourceParams(datasource: Class[_]): Array[ParamDefine] = {
    Array()
  }
}