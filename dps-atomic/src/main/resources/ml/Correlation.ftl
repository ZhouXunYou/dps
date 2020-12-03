package ${packagePath}

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.stat._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row,Dataset}
import org.apache.spark.sql.SparkSession

import dps.atomic.impl.AbstractAction


class ${className}(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {

	override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[Map[String, Any]]]
        val vectorsRDD: RDD[Tuple1[Vector]] = rdd.map(map => {
            ${correlationCode}
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
}