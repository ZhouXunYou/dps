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

//  val so = new SessionOperation("org.postgresql.Driver", "192.168.36.186", "5432", "postgres", "postgres", "postgres", "dps")
  val so = new SessionOperation("org.postgresql.Driver", "192.168.11.200", "5432", "postgres", "postgres", "postgres", "dps201")
  val ml = new MissionLoader(so)
  val outputPath = "D:\\source\\others\\dps\\dps-mission";
  val templatePath = "D:\\source\\others\\dps\\dps-atomic\\src\\main\\resources"
  val missionCode = "yltest"
//  val outputPath = "E:\\workspace\\scala.workspace\\dps\\dps-mission";
//  val templatePath = "E:\\workspace\\scala.workspace\\dps\\dps-atomic\\src\\main\\resources"
//  val missionCode = "DataProcessTask"
  //  @Test
  //  def testMissioJsonOutput {
  //    val mission = ml.getMission("DataProcessTask")
  //    val missionJson = JsonUtils.output(mission)
  //    assertNotNull(missionJson)
  //    println(missionJson)
  //  }

  @Test
  def testMissioOutputSource {
    val mission = ml.getMission(missionCode)
    val sg = new SourceGenerator(mission)
    sg.produce(outputPath, templatePath)
  }

  //    @Test
  //    def testKO() = assertTrue(false)

}


