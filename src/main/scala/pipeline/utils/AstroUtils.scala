package pipeline.utils

object AstroUtils {

  val PiDeg: Double = 180.0
  val Deg: Double = math.Pi / PiDeg
  val Arcmin: Double = Deg / 60.0

  def wcsDistance(coord1: Array[Double], coord2: Array[Double]): Double = {
    val coord1_deg = coord1.map(_ * Deg)
    val coord2_deg = coord2.map(_ * Deg)

    val distance = math.sin(coord1_deg(1)) * math.sin(coord2_deg(1)) +
      math.cos(coord1_deg(1)) * math.cos(coord2_deg(1)) * math.cos(coord1_deg(0) - coord2_deg(0))

    if (distance > -1.0) {
      if (distance < 1.0) math.acos(distance) / Deg else 0.0
    } else {
      PiDeg
    }
  }
}
