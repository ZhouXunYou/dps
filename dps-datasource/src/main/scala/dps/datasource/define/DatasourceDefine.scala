package dps.datasource.define

class DatasourceDefine(var datasourceName: String, var datasourceDefinParams: Map[String, DatasourceParamDefine],val streamProcess:Int) {
  var id:String = _
}