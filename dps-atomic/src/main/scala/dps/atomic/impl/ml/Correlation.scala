package dps.atomic.impl.ml

import scala.collection.mutable.Map
import org.apache.spark.ml.linalg.{ Matrix, Vectors,Vector }
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
// $example off$
import org.apache.spark.sql.SparkSession
import dps.atomic.impl.AbstractAction
import org.apache.spark.SparkConf
import dps.atomic.define.AtomOperationDefine
import org.apache.spark.rdd.RDD

/**
 * An example for computing correlation matrix.
 * Run with
 * {{{
 * bin/run-example ml.CorrelationExample
 * }}}
 */
class Correlation(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val rdd = this.pendingData.asInstanceOf[RDD[Map[String, Any]]]
        val vectorsRDD = rdd.map(map => {
            
        })

        return null
    }
    private def buildSparseVector(vectorLength:Int,indices:Array[Int],values:Array[Double]): Vector = {
        Vectors.sparse(vectorLength, indices, values)
    }
    private def buildSparseVector(values:Array[Double]): Vector = {
        Vectors.dense(values)
    }
    override def define: AtomOperationDefine = {
        return null
    }
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("CorrelationExample")
            .master("local[*]")
            .getOrCreate()
        import spark.implicits._

        // $example on$
        val data = Seq(
            //            Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
            Vectors.dense(4.0, 5.0, 0.0, 3.0),
            Vectors.dense(6.0, 7.0, 0.0, 8.0)
        //            Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
        )
        //        Correlation.c
        //        val df = data.map(Tuple1.apply).toDF("features")
        //        df.show()
        //        val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
        //        println(s"Pearson correlation matrix:\n $coeff1")
        //
        //        val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
        //        println(s"Spearman correlation matrix:\n $coeff2")
        // $example off$

        spark.stop()
    }
}
// scalastyle:on println
