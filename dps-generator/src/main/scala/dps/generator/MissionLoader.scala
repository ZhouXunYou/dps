package dps.generator

import data.process.util.SessionOperation
import dps.atomic.model.Mission
import dps.atomic.model.Datasource
import dps.atomic.model.OperationGroup
import scala.collection.mutable.Map
import scala.collection.immutable.List
import dps.atomic.model.Operation
import dps.atomic.model.MissionParam
import dps.atomic.model.OperationParam
import dps.atomic.model.DatasourceParam

class MissionLoader(val so: SessionOperation) {
  def getMission(missionName: String): Mission = {

    val missionDatas = so.executeQuery(
      "select m.id,m.mission_name,md.mission_type,md.mission_type_code from b_mission m inner join s_mission_def md on m.mission_def_id = md.id where m.mission_code = ?",
      Array(missionName))
    if (missionDatas != null && missionDatas.length == 1) {
      val mission = new Mission
      val missionData = missionDatas.apply(0)
      mission.id = missionData.get("id").get.asInstanceOf[String]
      mission.missionName = missionData.get("mission_name").get.toString()
      mission.missionCode = missionName
      mission.missionType = missionData.get("mission_type").get.toString()
      mission.missionTypeCode = missionData.get("mission_type_code").get.asInstanceOf[String]

      mission.missionParams = getMissionParams(mission.id)
      mission.datasources = getMissionDatasources(mission.id)
      mission.operationGroups = getOperationGroups(mission.id)
      return mission
    } else {
      return null
    }
  }

  def getMissionParams(missionId: String): Array[MissionParam] = {
    val missionParamDatas = so.executeQuery(
      "select mp.mission_id, mp.id, mpd.param_code, mpd.default_value, mp.mission_param_value, mpd.required from b_mission_param mp right join s_mission_param_def mpd on mp.mission_param_def_id = mpd.id where mp.mission_id = ? or mp.mission_id is null", Array(missionId))
    var missionParams = List[MissionParam]()
    missionParamDatas.foreach(missionParamData => {
      val missionParam = new MissionParam
      val paramCode = missionParamData.get("param_code").get
      val paramValue = missionParamData.get("mission_param_value").get
      val defaultValue = missionParamData.get("default_value").get
      val required = missionParamData.get("required").get
      missionParam.paramName = paramCode.toString()
      if(paramValue!=null){
        missionParam.paramValue = paramValue.toString()
      }
      if(defaultValue!=null){
        missionParam.defaultValue = defaultValue.toString()
      }
      
      if (required.toString().equals("1")) {
        missionParam.required = true
      } else {
        missionParam.required = false
      }
      missionParams = missionParam :: missionParams
    })
    missionParams = missionParams.reverse
    if (missionParams.length > 0) {
      return missionParams.toArray
    } else {
      return null
    }
  }
  /**
   * 获取任务数据源
   */
  private def getMissionDatasources(missionId: String): List[Datasource] = {
    var datasources = List[Datasource]()
    val datasourceDatas = so.executeQuery("select mds.id seq_id,dd.*,mds.datasource_variable_key from b_mission_datasource_seq mds inner join s_datasource_define dd on mds.datasource_def_id = dd.id where mds.mission_id = ? order by mds.seq_num", Array(missionId))
    datasourceDatas.foreach(datasourceData => {
      val seqId = datasourceData.get("seq_id").get
      val datasource = new Datasource
      datasource.datasourceName = datasourceData.get("datasource_name").get.toString()
      datasource.implementClass = datasourceData.get("datasource_class").get.toString()
      datasource.datasourceVariableKey = datasourceData.get("datasource_variable_key").get.toString()
      val datasourceParamDatas = so.executeQuery(
        "select mpd.*,mdpv.param_value from b_mission_datasource_param_value mdpv inner join s_datasource_param_define mpd on mdpv.datasource_param_def_id = mpd.id where mdpv.mission_datasource_seq_id = ?",
        Array(seqId))
      val datasourceParams = Map[String,DatasourceParam]()
      datasourceParamDatas.foreach(datasourceParamData => {
        val key = datasourceParamData.get("datasource_param_code").get.asInstanceOf[String]
        val name = datasourceParamData.get("datasource_param_name").get.asInstanceOf[String]
        val value = datasourceParamData.get("param_value").get.asInstanceOf[String]
        val defaultValue = datasourceParamData.get("datasource_param_default_value").get.asInstanceOf[String]
        val datasourceParam = new DatasourceParam
        datasourceParam.paramName = name
        datasourceParam.paramValue = value
        datasourceParam.paramDefaultValue = defaultValue
        datasourceParams.put(key, datasourceParam)
      })
      datasource.params = datasourceParams
      datasources = datasource :: datasources
    })
    datasources.reverse
  }
  /**
   * 获取原子操作组
   */
  private def getOperationGroups(missionId: String): List[OperationGroup] = {
    var operationGroups = List[OperationGroup]()
    val operationGroupDatas = so.executeQuery("select * from b_mission_operation_group where mission_id = ? order by seq_num", Array(missionId))
    operationGroupDatas.foreach(operationGroupData => {
    	val operationGroupId = operationGroupData.get("id").get.asInstanceOf[String]
      val operationGroup = new OperationGroup
      operationGroup.operationGroupName  = operationGroupData.get("operation_group_name").get.asInstanceOf[String]
      operationGroup.operationGroupDesc = operationGroupData.get("operation_group_desc").getOrElse("").asInstanceOf[String]
    	operationGroup.operations = getGroupOperations(operationGroupId)
      operationGroups = operationGroup :: operationGroups
    })
    operationGroups.reverse
  }
  
  
  private def getGroupOperations(operationGroupId: String): List[Operation] = {
    var operations = List[Operation]()
    val operationDatas = so.executeQuery("select mogc.*,od.operation_name,od.operation_code,od.template from b_mission_operation_group_config mogc inner join s_operation_def  od on mogc.operation_def_id = od.id where mogc.operation_group_id = ? order by mogc.seq_num", Array(operationGroupId))
    operationDatas.foreach(operationData => {
      val id = operationData.get("id").get.asInstanceOf[String]
      val operationName = operationData.get("operation_name").get.asInstanceOf[String]
      val operationCode = operationData.get("operation_code").get.asInstanceOf[String]
      val template = operationData.get("template").get.asInstanceOf[String]
      val classQualifiedName = operationData.get("class_qualified_name").get.asInstanceOf[String]
      val inVariableKey = operationData.get("in_variable_key").get.asInstanceOf[String]
      val outVariableKey = operationData.get("out_variable_key").get.asInstanceOf[String]
      val operation = new Operation
      operation.operationName = operationName
      operation.operationCode = operationCode
      operation.template = template
      operation.classQualifiedName = classQualifiedName
      operation.inVariableKey = inVariableKey
      operation.outVariableKey = outVariableKey
      operation.operationParams = getOperationParams(id)
      operations = operation :: operations
    })
    operations.reverse
  }
  
  private def getOperationParams(operationGroupConfigId: String): Map[String,OperationParam] = {
    val operationParamValueDatas = so.executeQuery("select opd.*,mop.operation_param_value from b_mission_operation_param mop inner join s_operation_param_def opd on mop.operation_param_def_id = opd.id  where mop.operation_group_config_id = ?", Array(operationGroupConfigId))
    
    val operationParams = Map[String,OperationParam]()
    operationParamValueDatas.foreach(operationParamValueData => {
      val operationParamName = operationParamValueData.get("operation_param_name").get.asInstanceOf[String]
      val operationParamCode = operationParamValueData.get("operation_param_code").get.asInstanceOf[String]
      val operationParamDefaultValue = operationParamValueData.get("operation_param_default_value").get.asInstanceOf[String]
      val operationParamValue = operationParamValueData.get("operation_param_value").get.asInstanceOf[String]
      val operationParam = new OperationParam
      operationParam.operationParamName = operationParamName
      operationParam.operationParamDefaultValue = operationParamDefaultValue
      operationParam.operationParamValue = operationParamValue
      operationParams.put(operationParamCode, operationParam)
    })
    return operationParams
  }
  


}