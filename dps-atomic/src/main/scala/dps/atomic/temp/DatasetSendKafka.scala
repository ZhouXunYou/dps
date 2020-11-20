package dps.atomic.temp

import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.broadcast.Broadcast
import dps.atomic.util.KafkaSink
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringSerializer
import dps.atomic.impl.AbstractAction
import org.apache.kafka.common.serialization.StringSerializer

class DatasetSendKafka(override val sparkSession: SparkSession, override val sparkConf:SparkConf,override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf,inputVariableKey, outputVariableKey, variables) with Serializable {
  def doIt(params: Map[String, String]): Any = {
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", params.get("bootstrapServers").get)
        properties.setProperty("key.serializer", classOf[StringSerializer].getName)
        properties.setProperty("value.serializer", classOf[StringSerializer].getName)
        properties
      }
      import sparkSession.implicits
      val context = sparkSession.sparkContext
      context.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    val sendTopicName = params.get("sendTopicName").get
    
    val dataset = this.pendingData.asInstanceOf[Dataset[Row]]
    dataset.toJSON.rdd.foreachPartition(f=>{
      if (f.hasNext) {
    	  kafkaProducer.value.send(sendTopicName, f.next())
      }
    })
    
  }

  override def define: AtomOperationDefine = {
    val params = Map(
      "bootstrapServers" -> new AtomOperationParamDefine("Bootstrap Servers","127.0.0.1:9092", true, "3"),
      "sendTopicName" -> new AtomOperationParamDefine("Send Topic Name","topic", true, "3"))

    val atomOperation = new AtomOperationDefine("Dataset Send Kafka", "datesetSendKafka", "DatasetSendKafka.flt", params.toMap,classOf[Nothing],classOf[Nothing],classOf[Nothing],classOf[Nothing])
    atomOperation.id = "dataset_send_kafka"
    return atomOperation
  }

}