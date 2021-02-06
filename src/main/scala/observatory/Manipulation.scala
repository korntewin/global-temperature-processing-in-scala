package observatory

/**
  * 4th milestone: value-added information
  */
object Manipulation extends ManipulationInterface {

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {

    lazy val memory: Map[(Int, Int), Temperature] = (for {
      lat <- -89 to 90; lon <- -180 to 179
    } yield Location(lat, lon)).par.map {
      case loc: Location =>
        ((loc.lat.toInt, loc.lon.toInt), Visualization.predictTemperature(temperatures, loc))
    }.toMap.seq

    g: GridLocation => {
      val lat = g.lat
      val lon = g.lon
      memory((lat, lon))
    } : Temperature

  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {

    lazy val memories = temperaturess.map(makeGrid)

    g: GridLocation => {
      memories
        .map(func => func(g))
        .map((_, 1))
        .reduce((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2)) match {
          case (sumT, sumN) => sumT / sumN
        }
    } : Temperature

  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    lazy val gridTemp = makeGrid(temperatures)

    g : GridLocation => {
      gridTemp(g) - normals(g)
    }

  }


}

