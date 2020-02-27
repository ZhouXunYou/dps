package dps.generator

import dps.atomic.model.Mission
import freemarker.template.Configuration
import freemarker.template.TemplateExceptionHandler
import freemarker.cache.FileTemplateLoader
import java.io.File
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import java.util.Optional
import dps.utils.Common

class SourceGenerator(val mission: Mission) {
  val srcPath = Array("src","main","scala").mkString(File.separator)
  
  /**
	 * 生成代码
	 * @param rootPath - 代码输出的根目录
	 */
  def produce(outputRootPath: String,templatePath:String) {
    val cfg = new Configuration(Configuration.VERSION_2_3_23);
    cfg.setDefaultEncoding("UTF-8")
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER)
    val baseDir = new File(templatePath)
    if(!baseDir.exists()){
      baseDir.mkdirs()
    }
    val fileTemplateLoader = new FileTemplateLoader(baseDir);
    cfg.setTemplateLoader(fileTemplateLoader)
    mission.operationGroups.foreach(operationGroup=>{
      operationGroup.operations.foreach(operation=>{
        val templatName = operation.template
        if(templatName!=null && !"".equals(templatName)){
          val template = cfg.getTemplate(templatName);
        
          val pathNames = operation.classQualifiedName.split("\\.")
          val classPackage = pathNames.slice(0, pathNames.length-1)
          val packagePath = classPackage.mkString(File.separator)
          val className = pathNames.slice(pathNames.length - 1, pathNames.length).apply(0)
          val scalaFileName = className.concat(".scala")
          val dir = new File(outputRootPath.concat(File.separator).concat(srcPath).concat(File.separator).concat(packagePath))
          if(!dir.exists()){
            dir.mkdirs()
          }
          val operationClassFile = new File(dir,scalaFileName)
          val out = new OutputStreamWriter(new FileOutputStream(operationClassFile));
          import java.util.{Map => JavaMap }
          import java.util.{HashMap => JavaHashMap }
          val templateParams:JavaMap[String,String] = new JavaHashMap
          operation.operationParams.foreach(operationParam=>{
            val param = operationParam._2
            templateParams.put(operationParam._1, Optional.ofNullable(param.operationParamValue).orElse(param.operationParamDefaultValue))
          })
          templateParams.put("packagePath", classPackage.mkString("."))
          templateParams.put("className", className)
          template.process(templateParams, out)
          out.close()
        }
      })
    })
    import java.util.{Map => JavaMap }
    import java.util.{HashMap => JavaHashMap }
    val completeActionTemplate = cfg.getTemplate("CompleteAction.flt");
    val completeActionTemplateParams:JavaMap[String,String] = new JavaHashMap
    
    completeActionTemplateParams.put("packagePath", "dps.mission.action")
    completeActionTemplateParams.put("missionCode", Common.getHumpName(mission.missionCode))
    completeActionTemplateParams.put("finishedCode", mission.finishedCode)
    
    val dir = new File(outputRootPath.concat(File.separator).concat(srcPath).concat(File.separator).concat(Array("dps","mission","action").mkString(File.separator)))
    val missionClassFile = new File(dir,s"${Common.getHumpName(mission.missionCode)}CompleteAction.scala")
    val out = new OutputStreamWriter(new FileOutputStream(missionClassFile));
    completeActionTemplate.process(completeActionTemplateParams, out)
  }
  /**
	 * 生成代码，输出路径为当前的类加载路径
	 */
  def produce(templatePath:String){
    val outputRootPath = this.getClass.getClassLoader.getResource("").getFile
    produce(outputRootPath,templatePath)
  }
  
}