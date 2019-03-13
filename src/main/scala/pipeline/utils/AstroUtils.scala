package pipeline.utils

object AstroUtils {

  val PiDeg: Double = 180.0
  val Deg: Double = math.Pi / PiDeg
  val Arcmin: Double = Deg / 60.0

  def wcsDistance(coord1: Array[Double], coord2: Array[Double]): Double = {
    val firstCoordDeg = coord1.map(_ * Deg)
    val secondCoordDeg = coord2.map(_ * Deg)

    val distance = math.sin(firstCoordDeg(1)) * math.sin(secondCoordDeg(1)) +
      math.cos(firstCoordDeg(1)) * math.cos(secondCoordDeg(1)) * math.cos(firstCoordDeg(0) - secondCoordDeg(0))

    if (distance > -1.0) {
      if (distance < 1.0) math.acos(distance) / Deg else 0.0
    } else {
      PiDeg
    }
  }
}
