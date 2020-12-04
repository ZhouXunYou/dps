package dps.atomic.define

class AtomOperationHasUdfDefine(override val id:String, override val operationName: String, override val operationCode: String, override val template: String, override val operationParams: Map[String, AtomOperationParamDefine], override val inputType: Class[_], override val outputType: Class[_], override val inputGenericType: Class[_], override val outputGenericType: Class[_], val udfs: Seq[AtomOperationUdf]) extends AtomOperationDefine(id, operationName, operationCode, template, operationParams, inputType, outputType, inputGenericType, outputGenericType) with Serializable {

}