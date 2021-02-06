package observatory

import java.time.LocalDate
//import org.apache.log4j.{Level, Logger}
//import org.apache.parquet.it.unimi.dsi.fastutil.floats.Float2IntLinkedOpenHashMap
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{HashPartitioner, SparkContext}
//import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning

import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {

//  import observatory.GlobalSpark.sc
  import observatory.GlobalSparkSession.spark
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val stationsFile = "/stations.csv"
    val year = 2015
    val temperaturesFile = "/2015.csv"
//    val temp1 = locTempDataFrame(stationsFile, temperaturesFile)
//    temp1.collect()
//    val temp2 = temp1.as[LocationTemperatureRow].rdd
//    temp2.collect()
//      .map(row => (LocalDate.of(year, row.month, row.day), Location(row.lat, row.lon), row.temp)).take(10)
//    val iter1 = locateTemperatures(year, stationsFile, temperaturesFile)
//    println(iter1)
//    val iter = locationYearlyAverageRecords(iter1)
//    println(iter)

  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
      locateTemperatureRDD(year, stationsFile, temperaturesFile).collect()
  }

//  def locTempRdd(year: Year, stationsFile: String, temperaturesFile: String): RDD[(LocalDate, Location, Temperature)] = {
//    locTempDataFrame(stationsFile, temperaturesFile)
//      .as[LocationTemperatureRow].rdd
//      .map(row => (LocalDate.of(year, row.month, row.day), Location(row.lat, row.lon), row.temp))
//  }
//
//  def locTempDataFrame(stationsFile: String, temperaturesFile: String): DataFrame = {
//    val stationsDF = StationData.dataFrameFromFile(stationsFile)
//    val temperaturesDF = TemperatureData.dataFrameFromFile(temperaturesFile)
//    temperaturesDF.join(stationsDF, Seq("stdId", "wbanId"))
//  }

  def locateTemperatureRDD(
                            year: Year,
                            stationsFile: String,
                            temperaturesFile: String): RDD[(LocalDate, Location, Temperature)] = {

    val tempRdd = readTemperatureRDDV2(year, temperaturesFile)
      .map(row => (row.id, row))
      .partitionBy(new HashPartitioner(spark.sparkContext.defaultParallelism)).cache()

    val stationRdd = readStationRDDV2(stationsFile).map(row => (row.id, row))

    tempRdd.join(stationRdd).mapValues {
      case (tempRow, stationRow) => (tempRow.timestamp, stationRow.loc, tempRow.temp)
    }.map(_._2)

  }

  def filterStation()(row: String): Boolean = {
    val splitRow = row.split(",", -1)
    if (splitRow(2) == "" || splitRow(3) == "") false else true
  }

  def getRddFromResource(filename: String): RDD[String] = {
    spark.sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream(filename), "utf-8").getLines().toStream)
  }

  def readStationRDDV2(filename: String): RDD[StationRow] =
    getRddFromResource(filename).filter(filterStation()).map(StationData.parseStationData)

  def readTemperatureRDDV2(year: Year, filename: String): RDD[TemperatureRow] = {
    getRddFromResource(filename).map(TemperatureData.parseTemperatureData(year))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    spark.sparkContext.parallelize(records.toSeq)
      .map(row => (row._2, (row._2, row._3, 1)))
      .reduceByKey {
        case ((loc1, temp1, num1), (_, temp2, num2)) => (loc1, temp1 + temp2, num1+num2)
      }.map {
        case (loc, (_, sumTemp, num)) => (loc, sumTemp / num)
      }.collect()
  }

}
