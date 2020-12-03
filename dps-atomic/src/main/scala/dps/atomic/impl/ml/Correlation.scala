package dps.atomic.impl.ml

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.stat._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row,Dataset}
import org.apache.spark.sql.SparkSession

import dps.atomic.define.AtomOperationDefine
import dps.atomic.define.AtomOperationHasUdfDefine
import dps.atomic.define.AtomOperationParamDefine
import dps.atomic.define.AtomOperationUdf
import dps.atomic.impl.AbstractAction
class Correlation(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[Map[String, Any]]]

        val vectorsRDD: RDD[Tuple1[Vector]] = rdd.map(map => {
            
            val values = map.get("values").get.asInstanceOf[Array[Double]]
            val vectorLength = map.get("vectorLength").getOrElse(-1).toString().toInt
            if (vectorLength.!=(-1)) {
                val indices = map.get("indices").get.asInstanceOf[Array[Int]]
                return (buildSparseVector(vectorLength, indices, values))
            } else {
                return (buildDenseVector(values))
            }
        })
        import sparkSession.implicits._
        val corr =Correlation.corr(vectorsRDD.toDF("features"), "features")
        variables.put(outputVariableKey, corr)
    }
    private def buildSparseVector(vectorLength: Int, indices: Array[Int], values: Array[Double]): Vector = {
        Vectors.sparse(vectorLength, indices, values)
    }
    private def buildDenseVector(values: Array[Double]): Vector = {
        Vectors.dense(values)
    }
    override def define: AtomOperationDefine = {
        val params = Map(
            "correlationCode" -> new AtomOperationParamDefine("correlation.code", """
            val values = map.get("values").get.asInstanceOf[Array[Double]]
            val vectorLength = map.get("vectorLength").getOrElse(-1).toString().toInt
            if (vectorLength.!=(-1)) {
                val indices = map.get("indices").get.asInstanceOf[Array[Int]]
                return (buildSparseVector(vectorLength, indices, values))
            } else {
                return (buildDenseVector(values))
            }
""", true, scalaType))
        val udfs = Seq(
            new AtomOperationUdf("buildSparseVector", Seq(classOf[Int].getName, classOf[Array[Int]].getName, classOf[Array[Double]].getName)),
            new AtomOperationUdf("buildDenseVector", Seq(classOf[Array[Double]].getName)))
        val atomOperation = new AtomOperationHasUdfDefine(getId, getClassName, getClassSimpleName, s"ml/${getClassSimpleName}.ftl", params.toMap, classOf[RDD[_]], classOf[Dataset[_]], classOf[Map[String, Any]], classOf[Row], udfs)
        return atomOperation
    }
}
