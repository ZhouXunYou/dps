package dps.atomic.util

import java.io.File

import scala.collection.mutable.Map

import org.apache.spark.SparkContext

import data.process.util.SessionOperation
import dps.atomic.define.AtomOperationDefine
import dps.atomic.impl.AbstractAction

object AtomOperationUtil {
  
  private val packageName = "dps.atomic.impl"
  def main(args: Array[String]): Unit = {
    getAtomOperations.foreach(atomOperationClass=>{
      val action = atomOperationClass.getConstructor(classOf[SparkContext],classOf[String],classOf[String],classOf[Map[String,Any]]).newInstance(null,null,null,Map()).asInstanceOf[AbstractAction]
      initAtomOperationDefin(action.define())
    })
  }
  def initAtomOperationDefin(define:AtomOperationDefine){
    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://39.98.141.108:16632/dps", "postgres", "1qaz#EDC")
//    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://10.1.1.99:5432/dps", "postgres", "postgres")
    val operationParams = Array[Any](define.id,define.operationName,define.operationCode,define.template)
    so.executeUpdate("insert into s_operation_def(id,operation_name,operation_code,template) values (?,?,?,?)", operationParams)
    define.operationParams.foreach(operationParam=>{
      val operationParamCode = operationParam._1
      val operationParamDefine = operationParam._2
      val operationParamId = s"${define.id}_${operationParamCode}"
      var required:Integer = 0
      if(operationParamDefine.required){
        required = 1
      }
      val params = Array[Any](operationParamId,define.id,operationParamDefine.operationParamName,operationParamCode,operationParamDefine.operationParamDefaultValue,required,Integer.valueOf(operationParamDefine.operationParamType))
      so.executeUpdate("insert into s_operation_param_def(id,operation_def_id,operation_param_name,operation_param_code,operation_param_default_value,required,param_type) values(?,?,?,?,?,?,?)", params)
    })
  }
  def getAtomOperations(): Array[Class[_]] = {
    var atomOperations = List[Class[_]]()
    val url = AtomOperationUtil.getClass.getClassLoader.getResource(packageName.replace(".", "/"))
    val sourcePackage = new File(url.getFile)
    sourcePackage.listFiles().foreach(file=>{
      if(file.isFile() && file.getName.endsWith(".class")){
        val clazz = Class.forName(Array(packageName,file.getName.split("\\.").apply(0)).mkString("."))
        if(clazz.getSuperclass == classOf[AbstractAction]){
          atomOperations = clazz :: atomOperations
        }
      }
    })
    atomOperations.reverse.toArray
  }
}