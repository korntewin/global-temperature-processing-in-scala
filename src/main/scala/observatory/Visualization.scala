package observatory

//import com.sksamuel.scrimage.nio.JpegWriter
import com.sksamuel.scrimage.{Image, Pixel}

import scala.annotation.tailrec
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  import observatory.GlobalSparkSession.spark

  // declare consant
  val EARTH_RADIUS = 6371
  val P = 1
  type km = Double

  //
  val width = 360
  val height = 180

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */

  def main(arr: Array[String]): Unit = {
    ???
  }

  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val weightsArr = weightsV2(weight(greaterDistance))(temperatures, location)
    val closeWeightsArr = weightsArr.filter {
      case (w, _) => if (w.isInfinity) true else false
    }

    if (closeWeightsArr.nonEmpty) {
      closeWeightsArr.head._2
//      averageTemperatureV2(closeWeightsArr)
    } else {
      reduceTemperatureV2(weightsArr)
    }
  }

  def averageTemperature(rdd: RDD[(Double, Temperature)]): Temperature = {
    rdd.map {
      case (_, temp) => (temp, 1)
    }.reduce((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2)) match {
      case (sumT, numEle) => sumT/numEle.toDouble
    }
  }

  def averageTemperatureV2(arr: Iterable[(Double, Temperature)]): Temperature = {
    arr.map {
      case (_, temp) => (temp, 1)
    }.reduce((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2)) match {
      case (sumT, numEle) => sumT/numEle.toDouble
    }
  }

  def reduceTemperature(rdd: RDD[(Double, Temperature)]): Temperature = {
    rdd.map {
      case (w, temp) => (w * temp, w)
    }.reduce((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2)) match {
      case (sumWT, sumW) => sumWT/sumW
    }
  }

  def reduceTemperatureV2(arr: Iterable[(Double, Temperature)]): Temperature = {
    arr.map {
      case (w, temp) => (w * temp, w)
    }.reduce((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2)) match {
      case (sumWT, sumW) => sumWT/sumW
    }
  }

  def weight(calDistance: (Location, Location) => km)(loc1: Location, loc2: Location): Double = {
    val dist = calDistance(loc1, loc2)
    if (dist < 1) Double.PositiveInfinity
    else 1/math.pow(dist, P)
  }

  def weights(weight: (Location, Location) => Double)
             (temperatures: Iterable[(Location, Temperature)], location: Location): RDD[(Double, Temperature)] = {
    spark.sparkContext.parallelize(temperatures.toStream).map(pair => (weight(pair._1, location), pair._2))
  }

  def weightsV2(weight: (Location, Location) => Double)
             (temperatures: Iterable[(Location, Temperature)], location: Location): Iterable[(Double, Temperature)] = {
    temperatures.map(pair => (weight(pair._1, location), pair._2))
  }
  def greaterDistance(loc1: Location, loc2: Location): km = {

    if (loc1.lat == loc2.lat && loc1.lon == loc2.lon) 0
//    else if (loc1.lat == -loc2.lat && math.abs(loc1.lon - loc2.lon)==180) EARTH_RADIUS*math.Pi
    else {
      val expr2 = math.cos(math.toRadians(loc1.lat)) *
        math.cos(math.toRadians(loc2.lat)) *
        math.cos(math.toRadians(math.abs(loc1.lon - loc2.lon)))
      val expr1 = math.sin(math.toRadians(loc1.lat)) * math.sin(math.toRadians(loc2.lat))
      val dSigma = math.acos(expr1 + expr2)
      EARTH_RADIUS*dSigma
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {

    val ascPoints = points.toList.sortBy(_._1)

    @tailrec def calInd(ind: Int, points: List[(Temperature, Color)]): Int = {
      if (points.isEmpty) ind
      else if (value < points.head._1) ind
      else calInd(ind+1, points.tail)
    }

    val ind = calInd(0, ascPoints)

    ind match {
      case 0 => ascPoints.head._2
      case x if x == ascPoints.length => ascPoints(x-1)._2
      case _ => linearInterpolate(ascPoints(ind), ascPoints(ind-1), value)
    }

  }

  def linearInterpolate(p1: (Temperature, Color), p2: (Temperature, Color), value: Temperature): Color = {

    val color1 = p1._2
    val color2 = p2._2

//    val wa = 1 / math.abs(p1._1 - value)
//    val wb = 1 / math.abs(p2._1 - value)
//
//    def interp(x: Int, y: Int): Double =
//      (wa*x + wb*y) / (wa + wb)
//
//    val interRed = interp(color1.red, color2.red)
//    val interGreen = interp(color1.green, color2.green)
//    val interBlue = interp(color1.blue, color2.blue)
    val mRed = (color2.red - color1.red)/(p2._1 - p1._1)
    val mGreen = (color2.green- color1.green)/(p2._1 - p1._1)
    val mBlue = (color2.blue - color1.blue)/(p2._1 - p1._1)

    val interRed = (value - p1._1) * mRed + color1.red
    val interGreen = (value - p1._1) * mGreen + color1.green
    val interBlue = (value - p1._1) * mBlue + color1.blue

    Color(interRed.round.toInt, interGreen.round.toInt, interBlue.round.toInt)

  }
  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    val gridRdd: RDD[(Int, Location)] = generateGrid(width, height)

    val pixels = generateTemperatures(gridRdd, temperatures)
      .mapValues(interpolateColor(colors, _))
      .mapValues(c => Pixel(c.red, c.green, c.blue, alpha=1))
      .sortByKey().map(_._2).collect()

    Image(width, height, pixels)

  }

  def generateTemperatures(gridRdd: RDD[(Int, Location)],
                           temperatures: Iterable[(Location, Temperature)]): RDD[(Int, Temperature)] =
    gridRdd.mapValues(predictTemperature(temperatures, _))

  def generateGrid(w: Int, h: Int): RDD[(Int, Location)] = {
    val grid = for(y <- (0 until h).par; x <- (0 until w).par) yield(y*h+x, xyToLatlon(x,y))
    spark.sparkContext.parallelize(grid.toStream)
  }

  def xyToLatlon(x: Int, y: Int): Location = Location(height/2- y, x - width/2)

}

