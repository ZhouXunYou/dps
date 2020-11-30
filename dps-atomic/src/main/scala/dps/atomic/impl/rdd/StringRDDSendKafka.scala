package dps.atomic.impl.rdd

import java.util.ArrayList
import dps.atomic.define.{ AtomOperationDefine, AtomOperationParamDefine }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.Map
import org.apache.spark.SparkConf
import dps.atomic.impl.AbstractAction
import org.apache.spark.broadcast.Broadcast
import dps.atomic.util.KafkaSink
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer

class StringRDDSendKafka(override val sparkSession: SparkSession, override val sparkConf: SparkConf, override val inputVariableKey: String, override val outputVariableKey: String, override val variables: Map[String, Any]) extends AbstractAction(sparkSession, sparkConf, inputVariableKey, outputVariableKey, variables) with Serializable {
    override def doIt(params: Map[String, String]): Any = {
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
        val rdd = this.pendingData.asInstanceOf[RDD[String]]
        rdd.foreach(value => {
            kafkaProducer.value.send(sendTopicName, value)
        })
    }

    override def define: AtomOperationDefine = {
        val params = Map(
            "bootstrapServers" -> new AtomOperationParamDefine("bootstrap.ervers", "127.0.0.1:9092", true, stringType),
            "sendTopicName" -> new AtomOperationParamDefine("send.topic.name", "topic", true, stringType))

        val atomOperation = new AtomOperationDefine(getId, getClassName, getClassSimpleName, s"rdd/${getClassSimpleName}.ftl", params.toMap, classOf[RDD[_]], classOf[Nothing], classOf[String], classOf[Nothing])
        return atomOperation
    }
}