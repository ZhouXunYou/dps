package dps.generator

import dps.utils.SessionOperation
import dps.atomic.model.Mission
import dps.atomic.model.Datasource
import dps.atomic.model.OperationGroup
import scala.collection.mutable.Map
import scala.collection.immutable.List
import dps.atomic.model.Operation
import dps.atomic.model.MissionParam
import dps.atomic.model.OperationParam
import dps.atomic.model.DatasourceParam
import scala.collection.mutable.ListBuffer

class MissionLoader(val so: SessionOperation) {
    def getMission(missionName: String): Mission = {

        val missionDatas = so.executeQuery(
            "select m.id,m.mission_name,md.mission_type,md.mission_type_code,m.finished_code,m.stream_batch,m.impl_class from b_mission m inner join s_def_mission md on m.mission_def_id = md.id where m.mission_code = ?",
            Array(missionName))
        if (missionDatas != null && missionDatas.size == 1) {
            val mission = new Mission
            val missionData = missionDatas.apply(0)
            //填充任务基础参数
            mission.id = missionData.get("id").get.asInstanceOf[String]
            mission.missionName = missionData.get("mission_name").get.toString()
            mission.missionCode = missionName
            mission.missionType = missionData.get("mission_type").get.asInstanceOf[String]
            mission.missionTypeCode = missionData.get("mission_type_code").get.asInstanceOf[String]
            mission.finishedCode = missionData.get("finished_code").getOrElse("").asInstanceOf[String]
            mission.streamBatch = missionData.get("stream_batch").getOrElse("").asInstanceOf[String]
            mission.implClass = missionData.get("impl_class").getOrElse("").asInstanceOf[String]
            //填充任务执行参数
            mission.missionParams = getMissionParams(mission.id)
            //填充原子操作组
            mission.operationGroups = getOperationGroups(mission.id)
            return mission
        } else {
            return null
        }
    }

    def getMissionParams(missionId: String): Array[MissionParam] = {
        val missionParamDatas = so.executeQuery(
            "select id,mission_id,mission_param_key,mission_param_value,data_type from b_mission_param_group where mission_id = ?", Array(missionId))
        var missionParams = ListBuffer[MissionParam]()
        missionParamDatas.foreach(missionParamData => {
            val missionParam = new MissionParam
            val id = missionParamData.get("id").get.asInstanceOf[String]
            val missionId = missionParamData.get("mission_id").get.asInstanceOf[String]
            val missionParamKey = missionParamData.get("mission_param_key").get.asInstanceOf[String]
            val missionParamValue = missionParamData.get("mission_param_value").get.asInstanceOf[String]
            val dataType = missionParamData.get("data_type").get.asInstanceOf[Int]
            missionParam.id = id
            missionParam.missionId = missionId
            missionParam.missionParamKey = missionParamKey
            missionParam.missionParamValue = missionParamValue
            missionParam.dataType = dataType
            missionParams.append(missionParam)
        })
        missionParams.toArray
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
            operationGroup.id = operationGroupId
            operationGroup.operationGroupName = operationGroupData.get("operation_group_name").get.asInstanceOf[String]
            operationGroup.operationGroupDesc = operationGroupData.get("operation_group_desc").getOrElse("").asInstanceOf[String]
            operationGroup.operations = getGroupOperations(operationGroupId)
            operationGroups = operationGroup :: operationGroups
        })
        operationGroups.reverse
    }

    private def getGroupOperations(operationGroupId: String): List[Operation] = {
        var operations = List[Operation]()
        val operationDatas = so.executeQuery("select mogc.*,od.operation_name,od.operation_code,od.template from b_mission_operation_group_config mogc inner join s_def_operation  od on mogc.operation_def_id = od.id where mogc.operation_group_id = ? order by mogc.seq_num", Array(operationGroupId))
        operationDatas.foreach(operationData => {
            val id = operationData.get("id").get.asInstanceOf[String]
            val operationName = operationData.get("operation_name").get.asInstanceOf[String]
            val operationCode = operationData.get("operation_code").get.asInstanceOf[String]
            val template = operationData.get("template").get.asInstanceOf[String]
            val classQualifiedName = operationData.get("class_qualified_name").get.asInstanceOf[String]
            val inVariableKey = operationData.get("in_variable_key").get.asInstanceOf[String]
            val outVariableKey = operationData.get("out_variable_key").get.asInstanceOf[String]
            val operation = new Operation
            operation.id = id
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

    private def getOperationParams(operationGroupConfigId: String): Map[String, OperationParam] = {
        val operationParamValueDatas = so.executeQuery("select mop.id,opd.operation_def_id,opd.operation_param_name,opd.operation_param_code,opd.operation_param_default_value,opd.required,opd.param_type,mop.operation_param_value from b_mission_operation_param mop inner join s_def_operation_param opd on mop.operation_param_def_id = opd.id  where mop.operation_group_config_id = ?", Array(operationGroupConfigId))

        val operationParams = Map[String, OperationParam]()
        operationParamValueDatas.foreach(operationParamValueData => {
            val id = operationParamValueData.get("id").get.asInstanceOf[String]
            val operationParamName = operationParamValueData.get("operation_param_name").get.asInstanceOf[String]
            val operationParamCode = operationParamValueData.get("operation_param_code").get.asInstanceOf[String]
            val operationParamDefaultValue = operationParamValueData.get("operation_param_default_value").get.asInstanceOf[String]
            val operationParamValue = operationParamValueData.get("operation_param_value").get.asInstanceOf[String]
            val operationParam = new OperationParam
            operationParam.id = id;
            operationParam.operationParamName = operationParamName
            operationParam.operationParamDefaultValue = operationParamDefaultValue
            operationParam.operationParamValue = operationParamValue
            operationParams.put(operationParamCode, operationParam)
        })
        return operationParams
    }

}