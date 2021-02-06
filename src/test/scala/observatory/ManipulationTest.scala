package observatory

import org.scalatest.refspec.RefSpec
import org.junit.Assert._
import org.junit.Test

trait ManipulationTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("data manipulation", 4) _

  // Implement tests for methods of the `Manipulation` object

}

class ManipulationTestV2 extends RefSpec with VisualizationTestV2 {
  import observatory.Manipulation._

  trait GridTest {
    val g1 = GridLocation(0, 0)
    val g2 = GridLocation(1, 1)
    val g3 = GridLocation(45, 90)
  }

  def `makeGrid test`: Unit = new GridTest with CloseDistance with FarDistance with ColorTest {
    val locTemps = Iterable(tempLoc1, tempLoc2, tempLoc4)
    val gridTemp = makeGrid(locTemps)
    assert(gridTemp(g1) == 25, s"temperature at gridloc(0,0) should equal 25, but got ${gridTemp(g1)}")
    assert(gridTemp(g2) == 32, s"temperature at gridloc(2,2) should equal 32, but got ${gridTemp(g2)}")
  }

  def `average test`: Unit = new GridTest with CloseDistance with FarDistance with ColorTest {
    val locTemps2 = Iterable(tempLoc1, tempLoc2, tempLoc3)
    val locTemps = Iterable(tempLoc1, tempLoc2, tempLoc4)

    val avgGridTemp = average(Iterable(locTemps, locTemps2))
    assert(avgGridTemp(g2) == 46, s"average temperature for ${g2} should equal 46, but got ${avgGridTemp(g2)}")
  }

  def `deviation test`: Unit = new GridTest with CloseDistance with FarDistance with ColorTest {
    val locTemps2 = Iterable(tempLoc1, tempLoc2, tempLoc3)
    val locTemps = Iterable(tempLoc1, tempLoc2, tempLoc4)

    val avgGridTemp = average(Iterable(locTemps, locTemps2))
    val difGridTemp = deviation(locTemps, avgGridTemp)

    assert(difGridTemp(g2) == -14, s"diff temp should equal -14 at ${g2}, but got ${difGridTemp(g2)}")
  }

}

