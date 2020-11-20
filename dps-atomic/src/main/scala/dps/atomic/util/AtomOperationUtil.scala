package dps.atomic.util

import java.io.File

import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import dps.utils.SessionOperation
import dps.atomic.define.AtomOperationDefine
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import dps.atomic.define.AtomOperationHasUdfDefine
import dps.atomic.define.AtomOperationUdf

object AtomOperationUtil {

    private val rootPackage = "dps.atomic.impl"
    def main(args: Array[String]): Unit = {
        //driver: String, ip: String,port:String, user: String, password: String,dbType:String,dbName:String
        //    val so = new SessionOperation("org.postgresql.Driver", "192.168.36.186","5432", "postgres", "postgres","postgres","dps")
        //    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://10.1.1.99:5432/dps", "postgres", "postgres")
        //    val so = new SessionOperation("com.mysql.jdbc.Driver", "jdbc:mysql://39.98.141.108:16606/dps?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "1qaz#EDC")
        val so = new SessionOperation("org.postgresql.Driver", "192.168.11.200", "5432", "postgres", "postgres", "postgres", "dps201")
        so.executeUpdate("truncate table s_def_operation_param", Array())
        so.executeUpdate("truncate table s_def_operation_udf", Array())
        so.executeUpdate("truncate table s_def_operation", Array())
        val sparkConf = new SparkConf()
        sparkConf.setAppName("initAtomicData")
        sparkConf.setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        getAtomOperations.foreach(atomOperationClass => {
            val action = atomOperationClass.getConstructor(classOf[SparkSession], classOf[SparkConf], classOf[String], classOf[String], classOf[Map[String, Any]]).newInstance(sparkSession, sparkConf, "", "", Map()).asInstanceOf[AbstractAction]
            initAtomOperationDefin(action.define(), so)
        })
    }
    def initAtomOperationDefin(define: AtomOperationDefine, so: SessionOperation) {
        val operationParams = Array[Any](define.id, define.operationName, define.operationCode, define.template,define.inputType.getName,define.outputType.getName,define.inputGenericType.getName,define.outputGenericType.getName)
        so.executeUpdate("insert into s_def_operation(id,operation_name,operation_code,template,input_type,output_type,input_generic_type,output_generic_type) values (?,?,?,?,?,?,?,?)", operationParams)
        define.operationParams.foreach(operationParam => {
            val operationParamCode = operationParam._1
            val operationParamDefine = operationParam._2
            val operationParamId = s"${define.id}_${operationParamCode}"
            var required: Integer = 0
            if (operationParamDefine.required) {
                required = 1
            }
            val params = Array[Any](operationParamId, define.id, operationParamDefine.operationParamName, operationParamCode, operationParamDefine.operationParamDefaultValue, required, Integer.valueOf(operationParamDefine.operationParamType))
            so.executeUpdate("insert into s_def_operation_param(id,operation_def_id,operation_param_name,operation_param_code,operation_param_default_value,required,param_type) values(?,?,?,?,?,?,?)", params)
        })
        if (define.isInstanceOf[AtomOperationHasUdfDefine]) {
            val udfs = define.asInstanceOf[AtomOperationHasUdfDefine].udfs
            for (i <- 0 until udfs.size) {
                val udf = udfs.apply(i)
                val seq = i + 1
                val params = Array[Any](s"${define.id}_udf_${seq}", udf.udfName, udf.params.mkString(","),seq)
                so.executeUpdate("insert into s_def_operation_udf(id,udf_name,udf_params,seq) values(?,?,?,?)", params)
            }
        }
    }

    private def getAtomOperations(path: File, classes: ListBuffer[Class[_]]): Unit = {
        if (path.isFile() && path.getName.endsWith(".class")) {
            val beginIndex = path.getAbsolutePath.indexOf(rootPackage.replace(".", File.separator))
            if (beginIndex.!=(-1)) {
                val packageFullPath = path.getAbsolutePath.substring(beginIndex);
                val className = packageFullPath.substring(0, packageFullPath.lastIndexOf("."))
                val clazz = Class.forName(className.replace(File.separator, "."))
                if (clazz.getSuperclass == classOf[AbstractAction]) {
                    print(clazz.getName)
                    classes.append(clazz)
                }
            }
        } else {
            path.listFiles().foreach(file => {
                getAtomOperations(file, classes)
            })
        }
    }

    def getAtomOperations(): Array[Class[_]] = {
        val url = AtomOperationUtil.getClass.getClassLoader.getResource(rootPackage.replace(".", "/"))
        val classes = new ListBuffer[Class[_]]
        getAtomOperations(new File(url.getFile), classes)
        classes.toArray
    }
}