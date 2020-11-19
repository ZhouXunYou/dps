package dps.datasource.util

import java.io.File
import java.lang.reflect.Modifier

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import dps.atomic.Operator
import dps.datasource.DataSource
import dps.datasource.StreamDatasource
import dps.utils.SessionOperation
import scala.collection.mutable.ListBuffer

object DatasourceUtils {
    private val packageName = "dps.datasource"
    def main(args: Array[String]): Unit = {
        val so = new SessionOperation("org.postgresql.Driver", "192.168.11.200", "5432", "postgres", "postgres", "postgres", "dps201")

        //    val so = new SessionOperation("com.mysql.jdbc.Driver", "39.98.141.108", "16606", "root", "1qaz#EDC", "mysql", "dps")
        so.executeUpdate("truncate table s_datasource_param_define", Array())
        so.executeUpdate("truncate table s_datasource_define", Array())
        val sparkConf = new SparkConf()
        sparkConf.setAppName("initDatasourceData")
        sparkConf.setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        val operator = new Operator(null, sparkSession, sparkConf, Map[String, Any]())
        sparkConf.set("kafka.duration","300")
        getDatasources().foreach(datasource => {
            println(datasource.getName)
            val datasourceInstance = datasource.getConstructor(classOf[SparkSession], classOf[SparkConf], classOf[Operator]).newInstance(sparkSession, sparkConf, operator).asInstanceOf[DataSource]
            initDatasource(datasourceInstance, so)
        })
    }
    def initDatasource(datasource: DataSource, so: SessionOperation) {
        val define = datasource.define()
        val datasourceParams = Array[Any](define.id, define.datasourceName, datasource.getClass.getName)
        so.executeUpdate("insert into s_datasource_define(id,datasource_name,datasource_class) values (?,?,?)", datasourceParams)
        define.datasourceDefinParams.foreach(datasourceDefinParam => {
            val datasourceParamCode = datasourceDefinParam._1
            val datasourceParamDefine = datasourceDefinParam._2
            val id = s"${define.id}_${datasourceParamCode}"
            val params = Array[Any](id, define.id, datasourceParamCode, datasourceParamDefine.paramName, datasourceParamDefine.defaultValue)
            so.executeUpdate("insert into s_datasource_param_define(id,datasource_id,datasource_param_code,datasource_param_name,datasource_param_default_value) values(?,?,?,?,?)", params)
        })
    }

    private def getDatasources(path: File, classes: ListBuffer[Class[_]]): Unit = {
        if (path.isFile() && path.getName.endsWith(".class")) {
            val beginIndex = path.getAbsolutePath.indexOf(packageName.replace(".", File.separator))
            if (beginIndex.!=(-1)) {
                val packageFullPath = path.getAbsolutePath.substring(beginIndex);
                val className = packageFullPath.substring(0, packageFullPath.lastIndexOf("."))
                val clazz = Class.forName(className.replace(File.separator, "."))
                if ((clazz.getSuperclass == classOf[DataSource] || clazz.getSuperclass == classOf[StreamDatasource]) && !Modifier.isAbstract(clazz.getModifiers)) {
                    classes.append(clazz)
                }
            }
        } else {
            path.listFiles().foreach(file => {
                getDatasources(file, classes)
            })
        }
    }

    def getDatasources(): Array[Class[_]] = {
        val url = DatasourceUtils.getClass.getClassLoader.getResource(packageName.replace(".", "/"))
        val classes = new ListBuffer[Class[_]]
        getDatasources(new File(url.getFile), classes)
        classes.toArray
        //    var datasources = List[Class[_]]()
        //    val url = DatasourceUtils.getClass.getClassLoader.getResource(packageName.replace(".", "/"))
        //    val sourcePackage = new File(url.getFile)
        //    val fiels = sourcePackage.listFiles();
        //    sourcePackage.listFiles().foreach(file => {
        //      if (file.isFile() && file.getName.endsWith(".class")) {
        //        val clazz = Class.forName(Array(packageName, file.getName.split("\\.").apply(0)).mkString("."))
        //        if (!Modifier.isAbstract(clazz.getModifiers)) {
        //          if (clazz.getSuperclass == classOf[DataSource] || clazz.getSuperclass == classOf[StreamDatasource]) {
        //            datasources = clazz :: datasources
        //          }
        //        }
        //      }
        //    })
        //    datasources.reverse.toArray
    }
}