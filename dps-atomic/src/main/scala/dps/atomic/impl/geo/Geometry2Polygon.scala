package dps.atomic.impl.geo

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import com.vividsolutions.jts.geom.Geometry

import dps.atomic.impl.AbstractAction

class Geometry2Polygon(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
        val polygonSpatialRDD = ShapefileReader.geometryToPolygon(this.pendingData.asInstanceOf[SpatialRDD[Geometry]])
        variables.put(outputVariableKey, polygonSpatialRDD)
//        val geometryFactory = new GeometryFactory()
//        val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
//        val result = KNNQuery.SpatialKnnQuery(polygonSpatialRDD, pointObject, 1000, false)
//        val a = new SpatialRDD[Geometry]
        
//        sparkSession.sparkContext.parallelize(JavaConverters.asScalaIteratorConverter(result.iterator()).asScala.toSeq, 1)
        //        a.rawSpatialRDD = sparkSession.sparkContext.parallelize(JavaConverters.asScalaIteratorConverter(result.iterator()).asScala.toSeq,0)
    }
}