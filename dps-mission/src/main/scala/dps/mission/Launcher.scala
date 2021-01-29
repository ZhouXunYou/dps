package dps.mission

import java.util.Optional

import scala.collection.Seq
import scala.collection.mutable.Map

import org.apache.spark.sql.SparkSession

import dps.atomic.Operator
import dps.datasource.DataSource
import dps.datasource.StreamDatasource
import dps.generator.MissionLoader
import dps.utils.RunParam
import dps.utils.SessionOperation
import org.apache.spark.SparkConf
import dps.atomic.model.Mission

object Launcher {
    def buildConf(mission: Mission, runParams: Map[String, String]): SparkConf = {
        val missionParams = mission.missionParams
        val conf = new SparkConf
        //特殊参数处理
        runParams.remove("--missionName")
        runParams.remove("--driver")
        runParams.remove("--ip")
        runParams.remove("--port")
        runParams.remove("--user")
        runParams.remove("--password")
        runParams.remove("--dbType")
        runParams.remove("--dbName")
        //数据库配置参数
        missionParams.foreach(param => {
            conf.set(param.missionParamKey, param.missionParamValue)
        })
        //命令行执行参数，如果参数的key相同，视为命令行参数优先级高于数据库配置参数
        runParams.foreach(kv => {
            val key = kv._1.replace("--", "")
            val value = kv._2
            conf.set(key, value)
        })
        conf.setAppName(mission.missionCode)
        return conf
    }
    def buildSparkSession(conf: SparkConf, missionCode: String): SparkSession = {
        val builder = SparkSession.builder()
        builder.appName(missionCode)
        builder.config(conf)
        builder.getOrCreate()
    }
    def main(args: Array[String]): Unit = {
        val params = RunParam.parserArgements(args)
        
        val requiredKeys = Seq("--missionName", "--driver", "--ip", "--port", "--user", "--password", "--dbType", "--dbName")
        if (!RunParam.validRequiredArgements(params, requiredKeys)) {
            println(s"These params is required ${requiredKeys.mkString(",")}. Params format e.g: ${requiredKeys.mkString(" {value},")} {value}")
            println("System exit. Please check and try again")
            System.exit(1)
        }
        val missionCode = params.get("--missionName").get
        val so = new SessionOperation(params.get("--driver").get, params.get("--ip").get, params.get("--port").get, params.get("--user").get, params.get("--password").get, params.get("--dbType").get, params.get("--dbName").get)
        val ml = new MissionLoader(so)
        val mission = ml.getMission(missionCode)
        so.close()
        val sparkConf = buildConf(mission, params)
        val sparkSession = buildSparkSession(sparkConf, missionCode)
        //用于原子操作输出的容器
        val missionVariables = Map[String, Any]()
        val operator = new Operator(mission.operationGroups, sparkSession, sparkConf, missionVariables)
        val datasourceInstance = Class.forName(mission.implClass)
            .getConstructor(classOf[SparkSession], classOf[SparkConf], classOf[Operator])
            .newInstance(sparkSession, sparkConf, operator)
            .asInstanceOf[DataSource]
        datasourceInstance.read();
        if (datasourceInstance.isInstanceOf[StreamDatasource]) {
            datasourceInstance.asInstanceOf[StreamDatasource].start()
        } else {
            val completeAction = Class.forName(s"dps.mission.action.${mission.missionCode}CompleteAction").newInstance().asInstanceOf[CompleteAction]
            completeAction.finished(mission, params)
        }
    }
}