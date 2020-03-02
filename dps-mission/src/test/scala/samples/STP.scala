package samples
import java.lang.Long
class STP extends Serializable {
  var sourceAddress: String = _
  var targetAddress: String = _
  var sourcePort: Integer = _
  var targetPort: Integer = _
  var transportProto: String = _
  var applicationProto: String = _
  var packageCount: Integer = _
  var packageSize: Long = _
  
}