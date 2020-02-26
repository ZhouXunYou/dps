package samples

import org.junit._
import Assert._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit._
@Test
class AppTest {

  @Test
  def testOK() = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //    TimeUnit.
    val unit = HOURS
    val interval = 1
    val date = sdf.parse("2019-01-01 23:10:10")
    val calendar = Calendar.getInstance
    val endTime = unit match {
      case DAYS => {
        calendar.add(Calendar.DAY_OF_MONTH, interval)
        calendar.getTime
      }
      case HOURS => {
        calendar.add(Calendar.HOUR, interval)
        calendar.getTime
      }
      case MINUTES => {
        calendar.add(Calendar.MINUTE, interval)
        calendar.getTime
      }
      case SECONDS => {
        calendar.add(Calendar.SECOND, interval)
        calendar.getTime
      }
      case MICROSECONDS | MILLISECONDS | NANOSECONDS => {
        calendar.getTime
      }

    }
    println(endTime)
  }

  //    @Test
  //    def testKO() = assertTrue(false)

}


