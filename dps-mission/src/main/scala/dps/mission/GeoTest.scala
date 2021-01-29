package dps.mission



import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.Adapter
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.ImageGenerator
import org.datasyslab.geosparkviz.core.ImageSerializableWrapper
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
import org.datasyslab.geosparkviz.utils.ImageType

object GeoTest {
    def main(args: Array[String]): Unit = {

        var cnames = List[String]();
        for (i <- 0 to 999) {
            cnames = cnames :+ "c" + i
        }
        println(cnames)
        System.setProperty("geospark.global.charset", "UTF8")
        val conf = new SparkConf()
        conf.setAppName("GeoSparkRunnableExample") // Change this to a proper name
        conf.setMaster("local[*]") // Delete this if run in cluster mode
        // Enable GeoSpark custom Kryo serializer
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        GeoSparkSQLRegistrator.registerAll(sparkSession)
        GeoSparkVizRegistrator.registerAll(sparkSession)

        val spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, "C:\\Users\\ZhouX\\Desktop\\DREP\\01-Dev\\03-需求开发与管理\\需求调研材料\\岚山地质灾害")
        println(spatialRDD.analyze())
        val df = Adapter.toDf(spatialRDD, cnames.slice(0, 34), sparkSession)
        df.createOrReplaceTempView("df")
        //        val df = Adapter.toDf(spatialRDD, s 
        val pointDf = sparkSession.sql("SELECT ST_GeomFromWKT(geometry) pointshapes,* FROM df")
        pointDf.printSchema()
        pointDf.show()
        println(pointDf.count())
        pointDf.createOrReplaceTempView("point")
        val circleDf = sparkSession.sql("SELECT ST_Buffer(pointshapes,0.1) circleshapes,* FROM point where c11<=118.877778")
        circleDf.printSchema()
        circleDf.show()
        println(circleDf.count())
        circleDf.createOrReplaceTempView("circle")
        
        val result = sparkSession.sql("SELECT point.* FROM point, circle WHERE ST_Within(point.pointshapes,circle.circleshapes)")
        result.show()
        println(result.count())
        
        
        
//        val bound = sparkSession.sql("select ST_Envelope_Aggr(circle.circleshapes) as bound FROM circle")
//        bound.show()
//        bound.createOrReplaceTempView("bound")
//        val pixels = sparkSession.sql("SELECT pixel, circleshapes FROM circle LATERAL VIEW ST_Pixelize(circleshapes, 256, 256, (select bound from bound)) AS pixel")
//        pixels.show()
//        pixels.createOrReplaceTempView("pixels")
//        val pc = sparkSession.sql("SELECT ST_Colorize(11, 11, 'red') as color,pixel FROM pixels");
//        pc.createOrReplaceTempView("pc")
//        val image = sparkSession.sql("SELECT ST_Render(pixel, color) AS image, (SELECT ST_AsText(bound) FROM bound) AS boundary FROM pc")
//        image.createOrReplaceTempView("image")
//        val img = sparkSession.table("image").take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
//        val imageGenerator = new ImageGenerator
//        imageGenerator.SaveRasterImageAsLocalFile(img, "d:/out", ImageType.PNG)
        
        
        sparkSession.stop()
    }
}