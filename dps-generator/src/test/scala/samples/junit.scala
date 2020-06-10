package samples

import org.junit._
import Assert._
import dps.utils.SessionOperation
import dps.generator.MissionLoader
import dps.utils.JsonUtils
import dps.generator.SourceGenerator

@Test
class AppTest {
//  val so = new SessionOperation("org.postgresql.Driver", "jdbc:postgresql://10.1.1.99/dps", "postgres", "postgres")
  
  val so = new SessionOperation("org.postgresql.Driver", "192.168.36.186","5432", "postgres", "postgres","postgres","dps")
  val ml = new MissionLoader(so)
//  @Test
//  def testMissioJsonOutput {
//    val mission = ml.getMission("DataProcessTask")
//    val missionJson = JsonUtils.output(mission)
//    assertNotNull(missionJson)
//    println(missionJson)
//  }
  
  @Test
  def testMissioOutputSource {
    val mission = ml.getMission("DataProcessTask")
    val sg = new SourceGenerator(mission)
    sg.produce("E:\\workspace\\scala.workspace\\dps\\dps-mission","E:\\workspace\\scala.workspace\\dps\\dps-atomic\\src\\main\\resources")
  }

  //    @Test
  //    def testKO() = assertTrue(false)

}


