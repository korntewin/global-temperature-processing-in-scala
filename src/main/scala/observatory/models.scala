package observatory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import scala.io.Source

/**
  * Introduced in Week 1. Represents a location on the globe.
  *
  * @param lat Degrees of latitude, -90 ≤ lat ≤ 90
  * @param lon Degrees of longitude, -180 ≤ lon ≤ 180
  */
case class Location(lat: Double, lon: Double)

/**
  * Introduced in Week 3. Represents a tiled web map tile.
  * See https://en.wikipedia.org/wiki/Tiled_web_map
  * Based on http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
  * @param x X coordinate of the tile
  * @param y Y coordinate of the tile
  * @param zoom Zoom level, 0 ≤ zoom ≤ 19
  */
case class Tile(x: Int, y: Int, zoom: Int)

/**
  * Introduced in Week 4. Represents a point on a grid composed of
  * circles of latitudes and lines of longitude.
  * @param lat Circle of latitude in degrees, -89 ≤ lat ≤ 90
  * @param lon Line of longitude in degrees, -180 ≤ lon ≤ 179
  */
case class GridLocation(lat: Int, lon: Int)

/**
  * Introduced in Week 5. Represents a point inside of a grid cell.
  * @param x X coordinate inside the cell, 0 ≤ x ≤ 1
  * @param y Y coordinate inside the cell, 0 ≤ y ≤ 1
  */
case class CellPoint(x: Double, y: Double)

/**
  * Introduced in Week 2. Represents an RGB color.
  * @param red Level of red, 0 ≤ red ≤ 255
  * @param green Level of green, 0 ≤ green ≤ 255
  * @param blue Level of blue, 0 ≤ blue ≤ 255
  */
case class Color(red: Int, green: Int, blue: Int)

trait ReadCSV {

  import observatory.GlobalSparkSession.spark

  protected def readCsvAsDataFrame(filename: String, schema: StructType): DataFrame = {
    val fileAbsPath = getClass.getResource(filename).getPath
    val wdPath = System.getProperty("user.dir")
    val relativePath = fileAbsPath.replace(wdPath, "").tail

    spark.read.schema(schema).option("inferSchema", "false").csv(relativePath)
  }

}

// temperature read
case class TemperatureRow(id: (Int,Int), timestamp: LocalDate, temp: Temperature)

object TemperatureData extends ReadCSV {

  private[observatory] def linesFromFile(filename: String): List[String] = {

    Option(Source.fromFile(filename)) match {
      case None => sys.error(s"$filename file is not existed")
      case Some(resource) => resource.getLines().toList
    }
  }

  private[observatory] def fahrenheitToCelcius(F: Temperature): Double = (F - 32.0) * (5.0/9.0)
  private[observatory] def fahrenheitToCelcius(F: Column): Column = (F - 32.0) * (5.0/9.0)

  private[observatory] def parseTemperatureData(year: Year)(row: String): TemperatureRow = {
    row.split(",", -1) match {
      case Array(stdId: String, wbanId: String, month: String, day: String, temp: String) =>
        (stdId, wbanId) match {
          case (sid, "") => TemperatureRow((sid.toInt, 0), LocalDate.of(year, month.toInt, day.toInt), fahrenheitToCelcius(temp.toDouble))
          case ("", wid) => TemperatureRow((0,wid.toInt), LocalDate.of(year, month.toInt, day.toInt), fahrenheitToCelcius(temp.toDouble))
          case (sid, wid) => TemperatureRow((sid.toInt,wid.toInt), LocalDate.of(year, month.toInt, day.toInt), fahrenheitToCelcius(temp.toDouble))
        }
    }
  }

  private[observatory] def dataFrameFromFile(filename: String): DataFrame = {
    val fields = "stdId wbanId month day temp"
    val schemaFields = fields
      .split(" ")
      .map(field =>
        if (field contains "Id") StructField(field, StringType, nullable=true)
        else if (field == "temp") StructField(field, DoubleType, nullable=false)
        else StructField(field, IntegerType, nullable=false)
      )
    val schema = StructType(schemaFields)

    readCsvAsDataFrame(filename, schema)
      .select(
        when(col("stdId").isNull, "").otherwise(col("stdId")).as("stdId"),
        when(col("wbanId").isNull, "").otherwise(col("wbanId")).as("wbanId"),
        col("month"),
        col("day"),
        fahrenheitToCelcius(col("temp")).as("temp")
      ).filter(
        col("month").isNotNull
          && col("day").isNotNull
          && col("temp").isNotNull
      )


  }

}

// station read
case class StationRow(id: (Int,Int), loc: Location)

object StationData extends ReadCSV {

  private[observatory] def linesFromFile(filename: String): List[String] = {

    Option(Source.fromFile(filename)) match {
      case None => sys.error(s"$filename file is not existed")
      case Some(resource) => resource.getLines().toList.filter(row => {
        val splitted = row.split(",", -1)
        if (splitted(2) == "" || splitted(3) == "") false else true
      })
    }
  }

  private[observatory] def parseStationData(row: String): StationRow = {
    row.split(",", -1) match {
      case Array(stdId: String, wbanId: String, lat: String, lon: String) =>
        (stdId, wbanId) match {
          case (sid, "") => StationRow((sid.toInt, 0), Location(lat.toDouble, lon.toDouble))
          case ("", wid) => StationRow((0, wid.toInt), Location(lat.toDouble, lon.toDouble))
          case (sid, wid) => StationRow((sid.toInt, wid.toInt), Location(lat.toDouble, lon.toDouble))
        }
    }
  }

  private[observatory] def dataFrameFromFile(filename: String): DataFrame = {
    val fields = "stdId wbanId lat lon"
    val schemaFields = fields
      .split(" ")
      .map(field =>
        if (field contains "Id") StructField(field, StringType, nullable=true)
        else StructField(field, DoubleType, nullable=false)
      )

    val schema = StructType(schemaFields)
    readCsvAsDataFrame(filename, schema)
      .select(
        when(col("stdId").isNull, "").otherwise(col("stdId")).as("stdId"),
        when(col("wbanId").isNull, "").otherwise(col("wbanId")).as("wbanId"),
        col("lat"),
        col("lon"),
      )
      .filter(
        col("lat").isNotNull && col("lon").isNotNull
      )

  }

}

object GlobalSpark {

  // delcare
  // suppress spark logger
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  // delcare sparkcontext
  val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("extractRdd")
  val sc: SparkContext = new SparkContext(sparkConf)

}

case class LocationTemperatureRow(stdId: String, wbanId: String,
                                  month: Int, day: Int, temp: Temperature, lat: Double, lon: Double)

object GlobalSparkSession {

  // delcare
  // suppress spark logger
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  // delcare sparkcontext
  val spark: SparkSession = SparkSession.builder().master("local").appName("extractRdd").getOrCreate()
}

object Colors {
  val colors = Iterable(
    (60.0, Color(255, 255, 255)),
    (32.0, Color(255, 0, 0)),
    (12.0, Color(255, 255, 0)),
    (0.0, Color(0, 255, 255)),
    (-15.0, Color(0, 0, 255)),
    (-27.0, Color(255, 0, 255)),
    (-50.0, Color(33, 0, 107)),
    (-60.0, Color(0, 0, 0))
  )
}

