package observatory

import com.sksamuel.scrimage.nio.PngWriter

import scala.math._
import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends InteractionInterface {

  import observatory.Visualization._
  import GlobalSparkSession.spark

  val width = 256
  val height = 256
  val zPixel = 8
  val zFactor = 1 << 8

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {

    val lat = toDegrees(
      atan(
        sinh(Pi - Pi * 2.0 * tile.y.toDouble / pow(2,tile.zoom))
      )
    )
    val lon = tile.x.toDouble / pow(2,tile.zoom) * 360.0 - 180.0

    Location(lat, lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val gridRdd = spark.sparkContext.parallelize(gridFromTile(tile))

    val pixels = gridRdd
      .mapValues(predictTemperature(temperatures, _))
      .mapValues(interpolateColor(colors, _))
      .mapValues(c => Pixel(c.red, c.green, c.blue, 127))
      .map(_._2).collect()

    Image(width, height, pixels)
  }

  def gridFromTile(tile: Tile): Vector[(Int, Location)] = {
    val X = tile.x
    val Y = tile.y
    val z = tile.zoom

    (for (addY <- 0 until height; addX <- 0 until width)
      yield {
        val t = tileLocation(Tile(X*zFactor + addX, Y*zFactor + addY, z + zPixel))
        (addY*height + addX, t)
      }).toVector
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {

    val desiredZoom = 4

    for (z <- 0 until desiredZoom) {
      generateSubTile(z).foreach(
        tile => yearlyData.foreach {
          case (year, data) => generateImage(year, tile, data)
        }
      )
    }
  }

  def generateSubTile(zoom: Int): List[Tile] = {
    (for(x <- 0 until 1 << zoom; y <- 0 until 1 << zoom) yield Tile(x, y, zoom)).toList
  }

//  def generateImage[Data](year: Year, tileLoc: Tile, temperatures: Iterable[(Location, Temperature)]): Unit = {
//
//    val img = tile(temperatures, Colors.colors, tileLoc)
//    val writer = new PngWriter(50)
//    img.output(s"target/temperatures/$year/${tileLoc.zoom}/${tileLoc.x}-${tileLoc.y}.png")(writer)
//
//  }

}
