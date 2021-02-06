package observatory

import com.sksamuel.scrimage.nio.JpegWriter
import org.scalatest.refspec.RefSpec

import scala.collection.concurrent.TrieMap
import org.junit.Assert._
import org.junit.Test

trait InteractionTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("interactive visualization", 3) _

}

trait CourseraTest {

  val locations = List(
    (Location(45.0, -90.0), 20.0),
    (Location(45.0, 90.0), 0.0),
    (Location(0.0, 0.0), 10.0),
    (Location(-45.0, -90.0), 0.0),
    (Location(-45.0, 90.0), 20.0)
  )
  val colorMap = List(
    (0.0, Color(255, 0, 0)),
    (10.0, Color(0, 255, 0)),
    (20.0 , Color(0  , 0  , 255))
  )
}

class InteractionTestV2 extends RefSpec with VisualizationTestV2 {

  import observatory.Interaction._
  import observatory.Extraction._

  trait TileSample {
    val t0 = Tile(0, 0, 0)
    val t1 = Tile(0, 0 ,1)
    val t2 = Tile(0, 0, 2)
    val t3 = Tile(0, 1, 1)
  }

  def `gridFromTile test`: Unit = new TileSample {
    val sampleGrid = gridFromTile(t1)
    assert(sampleGrid(0)._2.lon.toInt == -180,
      s"lon of the first element of samplegrid should equal to -180, but got ${sampleGrid(0)._2.lon.toInt}")
    println(sampleGrid.last)
  }

  def `tile test`: Unit = new TileSample with ColorTest with FarDistance {
    val img = tile(Iterable(tempLoc1, tempLoc2), Iterable(pairYellow, pairWhite, pairRed), t1)
//    val writer = new JpegWriter(50, true)
//    img.output("target/tiletest.jpeg")(writer)
    print(img.pixels.slice(0,3))
  }

  def `tile test 2`: Unit = new TileSample with ColorTest with FarDistance {
    val locTempAvg = locationYearlyAverageRecords(
      locateTemperatures(2021, "/stations_sample_m4.csv", "/2021_sample_m4.csv"))
    val img = tile(locTempAvg, Colors.colors, t0)
        val writer = new JpegWriter(50, true)
        img.output("target/tile0.jpeg")(writer)
  }
  def `tile test 3`: Unit = new TileSample with ColorTest with FarDistance {
    val locTempAvg = locationYearlyAverageRecords(
      locateTemperatures(2021, "/stations_sample_m4.csv", "/2021_sample_m4.csv"))
    val img = tile(locTempAvg, Colors.colors, t1)
    val writer = new JpegWriter(50, true)
    img.output("target/tile1.jpeg")(writer)
  }
  def `tile test 4`: Unit = new TileSample with ColorTest with FarDistance {
    val locTempAvg = locationYearlyAverageRecords(
      locateTemperatures(2021, "/stations_sample_m4.csv", "/2021_sample_m4.csv"))
    val img = tile(locTempAvg, Colors.colors, t3)
    val writer = new JpegWriter(50, true)
    img.output("target/tile2.jpeg")(writer)
  }

   def `visualization test`: Unit = new TileSample with CourseraTest {

     import observatory.Visualization._

     val img = tile(locations, colorMap, t0)
     val writer = new JpegWriter(50, true)
     img.output("target/courseraTest.jpeg")(writer)

     val pt: Location = Location(0, 90)
     val temperature = predictTemperature(locations, pt)
     val color = interpolateColor(colorMap, temperature)
     val w = weightsV2(weight(greaterDistance))(locations, pt)
     println((color, temperature))
     println(w)

//    assert(color.red>color.blue, "interpolateColor")
  }

}
