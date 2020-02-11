package dps.generator

import data.process.util.SessionOperation
import dps.utils.JsonUtils

object Test {
  def main(args: Array[String]): Unit = {
    /*
    --driver org.postgresql.Driver
		--jdbc jdbc:postgresql://192.168.36.186:5432/dap
		--user postgres
		--password postgres
		--missionName  ewqe
		--fileRootPath E:\workspace\platform.workspace\data-process
     */
    val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://10.1.1.99:5432/dps", "postgres", "postgres")
    val ml = new MissionLoader(so)
    val mission = ml.getMission("test")
    val sg = new SourceGenerator(mission)
    sg.produce("d:\\outfile")
//    mission.missionParams.foreach(mp => {
//      println(mp.paramName, mp.paramValue, mp.defaultValue, mp.required)
//    })
//    println(JsonUtils.output(ml))
//    //    println(JsonUtils.output(ml.getMission("test")))
  }
}