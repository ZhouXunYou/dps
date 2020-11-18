package dps.atomic.define

class AtomOperationHasUdfDefine(override val operationName: String, override val operationCode: String, override val template: String, override val operationParams: Map[String, AtomOperationParamDefine],val udfs: Seq[AtomOperationUdf]) extends AtomOperationDefine(operationName,operationCode,template,operationParams) with Serializable {
    
}