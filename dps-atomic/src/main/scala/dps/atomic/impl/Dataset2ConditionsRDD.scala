package dps.atomic.impl

import dps.atomic.define.{AtomOperationDefine, AtomOperationParamDefine}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.Map

class Dataset2ConditionsRDD(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {

  def doIt(params: Map[String, String]): Any = {
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]];

    val rdd: RDD[Tuple2[String, String]] = this.conditionHandle(dataset, false, false)

    this.variables.put(outputVariableKey, rdd);
  }

  /**
   * Dataset[Row]转RDD[Tuple2[String, String]]
   *
   * @param dataset
   * @param conditionExpr 条件表达式
   * @param logicalExpr   逻辑表达式
   * @return
   */
  private def conditionHandle(dataset: Dataset[Row], conditionExpr: Boolean, logicalExpr: Boolean): RDD[Tuple2[String, String]] = {
    dataset.rdd.groupBy(row => {
      row.getString(0)
    }).map(v => {
      var where = new String
      v._2.foreach(row => {
        var condition = "="
        var logical = " and "
        if (conditionExpr) {
          condition = row.getString(3)
        }
        if (logicalExpr) {
          if (row.getInt(4) == 0) {
            logical = " or "
          }
        }

        val current = row.getString(1) + condition + "'" + row.getString(2) + "'"
        if (where.isEmpty()) {
          where += current
        } else {
          where += logical.concat(current)
        }
      })
      Tuple2(v._1, where)
    })
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "conditionHandleCode" -> new AtomOperationParamDefine("Condition Handle Code",
        """
    dataset.rdd.groupBy(row => {
      row.getString(0)
    }).map(v => {
      var where = new String
      v._2.foreach(row => {
        var condition = "="
        var logical = " and "
        if (conditionExpr) {
          condition = row.getString(3)
        }
        if (logicalExpr) {
          if (row.getInt(4) == 0) {
            logical = " or "
          }
        }

        val current = row.getString(1) + condition + "'" + row.getString(2) + "'"
        if (where.isEmpty()) {
          where += current
        } else {
          where += logical.concat(current)
        }
      })
      Tuple2(v._1, where)
    })
    """, true, "3")
    )
    val atomOperation = new AtomOperationDefine("Dataset2ConditionsRDD", "dataset2ConditionsRDD", "Dataset2ConditionsRDD.flt", params.toMap)
    atomOperation.id = "dataset_conditions"
    return atomOperation
  }
}