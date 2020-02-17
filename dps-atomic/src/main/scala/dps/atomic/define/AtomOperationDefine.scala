package dps.atomic.define

class AtomOperationDefine(val operationName: String, val operationCode: String, val template: String, val operationParams: Map[String, AtomOperationParamDefine]) extends Serializable {
  var id: String = _
}