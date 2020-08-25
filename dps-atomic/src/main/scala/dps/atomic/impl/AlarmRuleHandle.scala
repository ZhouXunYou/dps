package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.Map

class AlarmRuleHandle(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]];

    //    val array = dataset.collect();
    //
    //    sparkSession.sparkContext.parallelize(array)
    //
    //    var map = Map[String, String]();
    //    if (array.size > 0) {
    //      map = this.dataHandle(array);
    //    }
    //
    //    this.variables.put(outputVariableKey, map);

    val rddMap = dataset.rdd.flatMap(row => {
      Map(row.getString(0) -> row)
    })

  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "dataHandleCode" -> new AtomOperationParamDefine("Data Handle Code",
        """
    val map = Map[String, String]()
    array.foreach(row => {
      val key = row.getString(5);
      val condition = map.get(key);
      var str = row.getString(3).concat("=").concat(row.getString(2));
      if (!condition.isEmpty) {
        str = condition.get.concat(" and ").concat(str);
      }
      map.put(key, str)
    })

    return map
    """, true, "3")
    )
    val atomOperation = new AtomOperationDefine("Dataset2ConditionsStr", "dataset2ConditionsStr", "Dataset2ConditionsStr.flt", params.toMap)
    atomOperation.id = "dataset_conditions"
    return atomOperation
  }
}