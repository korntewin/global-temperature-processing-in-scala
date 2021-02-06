package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.collection.parallel.mutable.ParArray


/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 extends Visualization2Interface {

  val ALPHA = 127

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    // interpolate on x axis
    val nx0 = (1 - point.x)*d00 + point.x*d10
    val nx1 = (1 - point.x)*d01 + point.x*d11

    point.y * nx1 + (1 - point.y)*nx0
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {

    val locs = Interaction.gridFromTile(tile)
    val pixels = generatePixels(grid, colors)(locs)

    Image(Interaction.width, Interaction.height, pixels)

  }

  def generatePixels(
                      grid: GridLocation => Temperature,
                      colors: Iterable[(Temperature, Color)]
                    )(locs: Vector[(Int, Location)]): Array[Pixel] = {

    locs.par.map {
      case (_, loc) => (locateToCell(loc), companionGridTemperature(grid)(loc))
    }.map {
      case (cell, temps) => bilinearInterpolation(cell, temps(0), temps(1), temps(2), temps(3))
    }
      .map(Visualization.interpolateColor(colors, _))
      .map(c => Pixel(c.red, c.green, c.blue, ALPHA)).toArray

  }

  def locateToCell(loc: Location): CellPoint = {
    CellPoint(loc.lon % 1, loc.lat % 1)
  }

  def companionGridTemperature(grid: GridLocation => Temperature)(loc: Location): Vector[Temperature] = {
    (for {
      lon <- loc.lon.toInt to loc.lon.toInt + 1
      lat <- loc.lat.toInt to loc.lat.toInt + 1
    } yield GridLocation(lat, lon))
      .map(grid).toVector
  }

}
