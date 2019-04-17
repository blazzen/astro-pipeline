package pipeline.utils

object AstroUtils {

  val PiDeg: Double = 180.0
  val Deg: Double = math.Pi / PiDeg
  val Arcmin: Double = Deg / 60.0

  def wcsDistance(coord1: Array[Double], coord2: Array[Double]): Double = {
    wcsDistance(coord1(0), coord1(1), coord2(0), coord2(1))
  }

  def wcsDistance(firstCoordRa: Double, firstCoordDec: Double, secondCoordRa: Double, secondCoordDec: Double): Double = {
    val firstCoordRaDeg = firstCoordRa * Deg
    val firstCoordDecDeg = firstCoordDec * Deg
    val secondCoordRaDeg = secondCoordRa * Deg
    val secondCoordDecDeg = secondCoordDec * Deg

    val distance = math.sin(firstCoordDecDeg) * math.sin(secondCoordDecDeg) +
      math.cos(firstCoordDecDeg) * math.cos(secondCoordDecDeg) * math.cos(firstCoordRaDeg - secondCoordRaDeg)

    if (distance > -1.0) {
      if (distance < 1.0) math.acos(distance) / Deg else 0.0
    } else {
      PiDeg
    }
  }
}
