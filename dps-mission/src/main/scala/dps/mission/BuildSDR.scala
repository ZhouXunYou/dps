package dps.mission

import java.lang.Short
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import java.lang.Long
import org.apache.spark.sql.SaveMode

object BuildSDR {
  def main(args: Array[String]): Unit = {
    val params = Map[String, String]("master" -> "local[*]", "dataSize" -> "1000", "storePath" -> "D:\\test\\parquet", "blockSize" -> s"${1024 * 1024 * 1024}", "cType" -> "snappy")
    if (args.size % 2 != 0) {
      println("参数错误")
    }
    for (result <- Range(0, args.size, 2)) {
      val key = args.apply(result);
      val value = args.apply(result + 1);
      params.put(key.replace("--", ""), value)
    }
    val dataSize = Integer.valueOf(params.get("dataSize").get)
    val storePath = params.get("storePath").get
    val blockSize = params.get("blockSize").get
    val master = params.get("master").get
    val cType = params.get("cType").get

    println(master, dataSize, storePath, blockSize, cType)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.parquet.compression.codec", cType)
    sparkConf.setAppName("SDRBuild")
    sparkConf.set("parquet.block.size", blockSize)
    sparkConf.setMaster(master)

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val seq = new Array[String](dataSize)
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val sdrRdd = sparkContext.parallelize(seq).map(f => {
      val startTime = Integer.valueOf(Tool.random(1556640000, 1564588800 - 1))
      val endTime = Integer.valueOf(startTime + Tool.random(1, 60))
      val sdrId = java.lang.Long.valueOf(Tool.randomString(11, "1,2,3,4,5,6,7,8,9"))
      val processType = Short.valueOf(Tool.randomString(1, "0,2"))
      val serviceType = Short.valueOf("1")
      val callingScene = Short.valueOf(Tool.random(1, 8).toString())
      val calledScene = Short.valueOf(Tool.random(1, 8).toString())
      val calling = Tool.randomMsdn()
      val called = Tool.randomMsdn()
      val oriCalled = called
      val result = Tool.random(1, 65535).asInstanceOf[Integer]
      val alertTime = Tool.random(-1, 7000000).asInstanceOf[Integer]
      val talkTime = Tool.random(-1, 400000000).asInstanceOf[Integer]
      val callingImsi = "4600" + Tool.randomString(11, "0,1,2,3,4,5,6,7,8,9")
      val calledImsi = "4600" + Tool.randomString(11, "0,1,2,3,4,5,6,7,8,9")
      val callingImei = Tool.randomString(1, "1,2,3,4,5,6,7,8,9") + Tool.randomString(14, "0,1,2,3,4,5,6,7,8,9")
      val calledImei = Tool.randomString(1, "1,2,3,4,5,6,7,8,9") + Tool.randomString(14, "0,1,2,3,4,5,6,7,8,9")
      val callingLacTac = Tool.random(0, 65535).asInstanceOf[Integer]
      val callingCellId = Tool.randomLong(0, 4294967295L).asInstanceOf[java.lang.Long]
      val calledLacTac = Tool.random(0, 65535).asInstanceOf[Integer]
      val calledCellId = Tool.randomLong(0, 4294967295L).asInstanceOf[java.lang.Long]
      val sourIp = Tool.randomIpNum().asInstanceOf[Integer]
      val destIp = Tool.randomIpNum().asInstanceOf[Integer]
      //      val callingUe
      //      val callingEnodeb
      //      val callingMme
      val callingMsc = Tool.random(-1, 3000000).asInstanceOf[Integer]
      val calledMsc = Tool.random(-1, 3000000).asInstanceOf[Integer]
      val relDir = Tool.random(0, 3).asInstanceOf[Integer]
      val alertingTime = Tool.random(1, 60).asInstanceOf[Integer]
      val connDelay = Tool.random(1, 60).asInstanceOf[Integer]
      val callingCity = Tool.random(0, 999).asInstanceOf[Integer]
      val callingUserType = 0.asInstanceOf[Integer]
      val callingTerminalFac = Tool.random(0, 10000).asInstanceOf[Integer]
      val callingTerminalType = Tool.random(0, 90000).asInstanceOf[Integer]
      val calledCity = Tool.random(0, 999).asInstanceOf[Integer]
      val callingNumType = Short.valueOf(Tool.random(1, 3).toString())
      val calledNumType = Short.valueOf(Tool.random(1, 3).toString())
      val callingOwnerProv = Tool.random(0, 1000).asInstanceOf[Integer]
      val callingOwnerCity = Tool.random(0, 999).asInstanceOf[Integer]
      val callingOwnerOperation = Tool.random(0, 1000).asInstanceOf[Integer]
      val calledOwnerProv = Tool.random(0, 1000).asInstanceOf[Integer]
      val calledOwnerCity = Tool.random(0, 999).asInstanceOf[Integer]
      val calledOperation = Tool.random(0, 1000).asInstanceOf[Integer]
      val callingArea = Tool.random(0, 50000).asInstanceOf[Integer]
      val calledArea = Tool.random(0, 50000).asInstanceOf[Integer]
      val date = new Date(startTime * 1000L)
      val partitionDay = Integer.valueOf(dateFormat.format(date))

      RowFactory.create(
        startTime, endTime, sdrId, processType, serviceType, callingScene, calledScene, calling, called,
        oriCalled, result, alertTime, talkTime, callingImsi, calledImsi, callingImei, calledImei, callingLacTac, callingCellId, calledLacTac, calledCellId, sourIp, destIp,
        callingMsc, calledMsc, relDir, alertingTime, connDelay, callingCity, callingUserType, callingTerminalFac, callingTerminalType, calledCity, callingNumType, calledNumType, callingOwnerProv, callingOwnerCity,
        callingOwnerOperation, calledOwnerProv, calledOwnerCity, calledOperation, callingArea, calledArea, partitionDay)
    })
    val schema = StructType.apply(Seq(
      StructField.apply("start_time", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("end_time", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("sdr_id", DataTypes.LongType, false, Metadata.empty),
      StructField.apply("process_type", DataTypes.ShortType, false, Metadata.empty),
      StructField.apply("service_type", DataTypes.ShortType, false, Metadata.empty),
      StructField.apply("calling_scene", DataTypes.ShortType, false, Metadata.empty),
      StructField.apply("called_scene", DataTypes.ShortType, false, Metadata.empty),
      StructField.apply("calling", DataTypes.StringType, false, Metadata.empty),
      StructField.apply("called", DataTypes.StringType, false, Metadata.empty),
      StructField.apply("ori_called", DataTypes.StringType, false, Metadata.empty),
      StructField.apply("_result", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("alert_time", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("talk_time", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_imsi", DataTypes.StringType, false, Metadata.empty),
      StructField.apply("called_imsi", DataTypes.StringType, false, Metadata.empty),
      StructField.apply("calling_imei", DataTypes.StringType, false, Metadata.empty),
      StructField.apply("called_imei", DataTypes.StringType, false, Metadata.empty),
      StructField.apply("calling_lac_tac", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_cell_id", DataTypes.LongType, false, Metadata.empty),
      StructField.apply("called_lac_tac", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("called_cell_id", DataTypes.LongType, false, Metadata.empty),
      StructField.apply("sour_ip", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("dest_ip", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_msc", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("called_msc", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("rel_dir", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("alerting_time", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("conn_delay", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_city", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_user_type", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_terminal_fac", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_terminal_type", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("called_city", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_num_type", DataTypes.ShortType, false, Metadata.empty),
      StructField.apply("called_num_type", DataTypes.ShortType, false, Metadata.empty),
      StructField.apply("calling_owner_prov", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_owner_city", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_owner_operation", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("called_owner_prov", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("called_owner_city", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("called_operation", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("calling_area", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("called_area", DataTypes.IntegerType, false, Metadata.empty),
      StructField.apply("partition_day", DataTypes.IntegerType, false, Metadata.empty)))
    val dataFrame = sqlContext.createDataFrame(sdrRdd, schema)
    dataFrame.show()
    dataFrame.coalesce(1).write
      .partitionBy("partition_day")
      .mode(SaveMode.Append)
      .parquet(storePath)
  }
}
