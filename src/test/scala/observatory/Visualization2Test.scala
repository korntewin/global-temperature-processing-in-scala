package observatory

import com.sksamuel.scrimage.Image
import com.sksamuel.scrimage.nio.PngWriter
import org.scalatest.refspec.RefSpec
import org.junit.Test

trait Visualization2Test extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("value-added information visualization", 5) _

  // Implement tests for methods of the `Visualization2` object

}

class Visualization2V2Test extends RefSpec {

  import Visualization2._
  import Manipulation._

  trait CellPointTest {
    val c1 = CellPoint(0.2, 0.3)
  }

  trait LocationTest {
    val compLoc1 = Location(0.2, 0.3)
    val compLoc2 = Location(51.22, 27.83)
  }

  def `bilinearInterpolation test`: Unit = new CellPointTest {
    val testTemp = bilinearInterpolation(
      c1,
      20, 25,
      30, 35
    )
    assert(testTemp == 23.5, s"test temp should equal 23.5, but got ${testTemp}")
  }

  def `locateToCell test`: Unit = new LocationTest {
    val cell1 = locateToCell(compLoc1)
    val cell2 = locateToCell(compLoc2)
    assert(cell1 == CellPoint(0.3, 0.2), s"${compLoc1} should return Cell(0.3, 0.2), but got ${cell1}")
  }

  def `companionGridTemperature test`: Unit = new LocationTest with FarDistance with CloseDistance with ColorTest {
    val grid = makeGrid(Iterable(tempLoc1, tempLoc2, tempLoc3))
    val companions = companionGridTemperature(grid)(compLoc1)
    assert(companions(0) == 25, s"top left temp should equal 25, but got ${companions(0)}")
    assert(companions(3) == 60, s"top left temp should equal 60, but got ${companions(3)}")
  }

  def `generatePixels test`: Unit = new CourseraTest {
    val grid = makeGrid(locations)
    val pixels = generatePixels(grid, colorMap)(Interaction.gridFromTile(Tile(0, 0, 0)))
    assert(pixels.length == 256*256)
  }

  def `visualizeGrid test`: Unit = new CourseraTest {
    val grid = makeGrid(locations)
    val img = visualizeGrid(grid, colorMap, Tile(0, 0, 0))
    val writer = PngWriter(5)
    img.output("target/visualizeGrid.png")(writer)
  }

}
