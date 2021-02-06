package observatory

import com.sksamuel.scrimage.nio.{ImageWriter, JpegWriter}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.refspec.RefSpec

trait VisualizationTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("raw data display", 2) _

}

// Implement tests for the methods of the `Visualization` object
trait FarDistance {
  val loc1 = Location(0, 0)
  val loc2 = Location(50, 70)
  val tempLoc1 = (loc1, 25.0)
  val tempLoc2 = (loc2, 30.0)
}

trait CloseDistance {
  val loc3 = Location(1, 1)
  val loc4 = Location(1, 1)
  val tempLoc3 = (loc3, 60.0)
  val tempLoc4 = (loc4, 32.0)
}

trait ColorTest {
  val pairWhite = (60.0, Color(255, 255, 255))
  val pairRed = (32.0, Color(255, 0, 0))
  val pairYellow = (12.0, Color(255, 255, 0))
}

trait VisualizationTestV2 extends RefSpec {

  import observatory.Visualization._


  def isClose(threshold: Double = 1e-9)(x: Double, y: Double): Boolean = {
    math.abs(x - y) <= (threshold*math.abs(x).max(math.abs(y))).max(0)
  }

  def `greaterDistance test`: Unit = new FarDistance with CloseDistance {

    val dist = greaterDistance(loc1, loc2)

    assert(isClose(1e-4)(dist, 8595.3671), s"Earth distance should equal to 8595.3671, but got ${dist}")

    val dist2 = greaterDistance(loc3, loc4)
    println(dist2)
  }

  def `weight Test`: Unit = new FarDistance with CloseDistance {
    val w = weight(greaterDistance)(loc1, loc2)
    assert(isClose(1e-4)(w, 1.1634e-4), s"weight should equal to 1.1634e-4, but got ${w}")

    val w2 = weight(greaterDistance)(loc3, loc4)
    assert(w2.isInfinity, s"weight for close distance should equal to infinity, but got ${w2}")
  }

  def `predictTemperature test`: Unit = new FarDistance with CloseDistance {
    val tempLocTest = predictTemperature(Iterable((loc1, 20), (loc2, 25)), loc3)
    assert(isClose(1e-4)(tempLocTest, 20.091461705203102), s"predict temp for far distance is 20.09146, but got ${tempLoc3}")

    val tempCloseLoc = predictTemperature(Iterable((loc1, 20), (loc2, 25), (loc3, 28)), loc4)
    assert(isClose(1e-4)(tempCloseLoc, 28), s"predict temp for far distance is 28, but got ${tempCloseLoc}")

    val tempCloseLoc2 = predictTemperature(Iterable((loc1, 20), (loc2, 25), (loc3, 28), (loc3, 30)), loc4)
    assert(isClose(1e-4)(tempCloseLoc2, 28), s"predict temp for far distance is 28, but got ${tempCloseLoc2}")
  }

  def `linearInterpolate test`: Unit = new ColorTest {
    val testColor = linearInterpolate(pairWhite, pairRed, 32)
    assert(testColor.red == 255 && testColor.green == 0 && testColor.blue == 0,
      s"temp 32 should return red color, but got $testColor")

    val testColor2 = linearInterpolate(pairWhite, pairRed, 46)
    assert(testColor2.red == 255 && testColor2.green ==  128 && testColor2.blue == 128,
      s"temp 32 should return Color(255, 128, 128), but got $testColor2")
  }

  def `interpolateColr test`: Unit = new ColorTest {
    val interColor = interpolateColor(Iterable(pairWhite, pairRed, pairYellow), 46)
    assertEquals(s"expected Color(255, 128, 128), but got $interColor", Color(255, 128, 128), interColor)
  }

  def `generate grid`: Unit = new ColorTest with FarDistance with CloseDistance {
    val grid = generateGrid(width, height)
    val gridRdd = generateTemperatures(grid, Iterable(tempLoc1, tempLoc3))
    val tempRes = gridRdd.collect().toList
    assert(tempRes(90*360 + 180)._2 == tempLoc1._2, s"temp at lat 0 lon 0 should equal to 25, but got ${tempLoc1._2}")
    assert(tempRes(89*360 + 181)._2 == tempLoc3._2, s"temp at lat 0 lon 0 should equal to 60, but got ${tempLoc3._2}")
  }

  def `visualize test`: Unit = new ColorTest with FarDistance with CloseDistance {
    val img = visualize(Iterable(tempLoc1, tempLoc3), Iterable(pairRed, pairWhite, pairYellow))
  }
}
