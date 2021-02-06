package observatory

import observatory.GlobalSpark.sc
import org.junit.Assert._
import org.junit.{Ignore, Test}
import org.scalatest.refspec.RefSpec

import java.time.LocalDate


trait ExtractionTest extends MilestoneSuite {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _

  // Implement tests for the methods of the `Extraction` object
}

trait CSVRead {
  val testTempFileName = "/2021_sample_m3.csv"
  val testStationFileName = "/stations_sample_m3.csv"
}

class ExtractionTestV2 extends RefSpec {

  import observatory.Extraction._

  def `locateTemperature test`: Unit = new CSVRead {
    val iter = locateTemperatures(2021, testStationFileName, testTempFileName)
    println(iter.toList)
//    assert(iter.toList.length == 10,
//      s"locate temperature dataframe from test file should equal 10, but got ${iter.toList.length}")
  }

  def `locationYearlyAverageRecord test`: Unit = new CSVRead {
    val iter = locateTemperatures(2021, testStationFileName, testTempFileName)
    val yearlyIter = locationYearlyAverageRecords(iter)
    println(yearlyIter.toList)
//    assert(yearlyIter.toList.length == 1,
//      s"locate temperature dataframe from test file should equal 10, but got ${yearlyIter.toList.length}")
  }

//  def `read temperatureV2 csv using RDD`: Unit = new CSVRead {
//    import observatory.Extraction._
//
//    val testRdd = readTemperatureRDDV2(sc, 1975, testTempFileName)
//    val testArr = testRdd.collect()
//    println(testArr(0).id)
//    println(testArr(0).timestamp)
//    println(testArr(0).temp)
//
//    assert(testArr.length == 10, "testArray should equal 10")
//  }
//
//  def `read stationV2 csv using RDD`: Unit = new CSVRead {
//    import observatory.Extraction._
//
//    val testRdd = readStationRDDV2(sc, testStationFileName)
//    val testArr = testRdd.collect()
//    println(testArr(0).id)
//    println(testArr(0).loc)
//
//    assert(testArr.length == 3, s"testArray length should equal 3, but got ${testArr.length}")
//  }
//
//  def `test locateTemperature using RDD`: Unit = new CSVRead {
//    import observatory.Extraction._
//
//    val testIterable = locateTemperatures(1975, testStationFileName, testTempFileName)
//
//    assert(testIterable.size == 10, s"test results size should equal to 10 but got ${testIterable.size}")
//    println(testIterable)
//  }
//
//  def `test locattionYearlyAverageRecord`: Unit = new CSVRead {
//    import observatory.Extraction._
//
//    val testLocateTemp = Iterable(
//      (LocalDate.of(2000,1,1), Location(0, 0), 15.0),
//      (LocalDate.of(2000,2,1), Location(0, 0), 17.0),
//      (LocalDate.of(2000,3,1), Location(1, 0), 33.0),
//    )
//    val testIterable = locationYearlyAverageRecords(testLocateTemp)
//
//    println(testIterable)
//  }

}

class DataImportTest extends RefSpec {

  import observatory.Extraction._

  def `StationData --> dataFrameFromFile test`: Unit = new CSVRead {
    val df = StationData.dataFrameFromFile(testStationFileName)
    df.show()
    df.printSchema()
    assert(df.count() == 3, s"station test data record should equal to 3, but got, ${df.count()} ")
  }

  def `TemperatureData --> dataFrameFromFile test`: Unit = new CSVRead {
    val df = TemperatureData.dataFrameFromFile(testTempFileName)
    df.show()
    df.printSchema()
    assert(df.count() == 10, s"temperature test data record should equal to 10, but got, ${df.count()} ")
  }

  def `JoinedDataFrame test`: Unit = new CSVRead {
    val df1 = TemperatureData.dataFrameFromFile(testTempFileName)
    val df2 = StationData.dataFrameFromFile(testStationFileName)

    val df3 = df1.join(df2, Seq("stdId", "wbanId"))
    df3.show()
    df3.printSchema()
    assert(df3.count() == 10, s"temperature test data record should equal to 10, but got, ${df3.count()} ")
  }
}