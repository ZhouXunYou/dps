package dps.atomic.define

class AtomOperationDefine(val id:String,val operationName: String, val operationCode: String, val template: String, val operationParams: Map[String, AtomOperationParamDefine],val inputType:Class[_],val outputType:Class[_],val inputGenericType:Class[_],val outputGenericType:Class[_]) extends Serializable {
}