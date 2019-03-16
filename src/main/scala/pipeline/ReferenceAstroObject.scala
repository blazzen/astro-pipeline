package pipeline

object ReferenceAstroObject {

  private val Deg = math.Pi / 180.0
  private val Arcsec = Deg / 3600.0
  private val Mas = Arcsec / 1000.0

  object GaiaDr2 {
    val Ra = "ra"
    val Dec = "dec"
    val RaError = "ra_error"
    val DecError = "dec_error"
    val RefEpoch = "ref_epoch"
    val PhotGMeanFlux = "phot_g_mean_flux"
    val PhotGMeanFluxError = "phot_g_mean_flux_error"
    val PhotGMeanMag = "phot_g_mean_mag"

    val Columns: Seq[String] = Seq(Ra, Dec, RaError, DecError, RefEpoch, PhotGMeanFlux, PhotGMeanFluxError, PhotGMeanMag)
  }

  def fromGaiaDr2(ra: Double, dec: Double, raError: Double, decError: Double, refEpoch: Double,
                  photGMeanFlux: Double, photGMeanFluxError: Double, photGMeanMag: Double): ReferenceAstroObject = {
    var mag = photGMeanMag
    var magError = 1.0857 * photGMeanFluxError / photGMeanFlux
    if (photGMeanFlux <= 0.0) {
      mag = 99.0
      magError = 99.0
    }

    ReferenceAstroObject(ra, dec, raError * Mas / Deg, decError * Mas / Deg, mag, magError, refEpoch)
  }

  def apply(line: String): ReferenceAstroObject = {
    val columns = line.split(",", -1).map(_.trim)
    val arr = columns.map(_.toDouble)

    apply(arr)
  }

  def apply(arr: Array[Double]): ReferenceAstroObject =
    fromGaiaDr2(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))

  def apply(seq: Seq[Double]): ReferenceAstroObject =
    fromGaiaDr2(seq(0), seq(1), seq(2), seq(3), seq(4), seq(5), seq(6), seq(7))
}

case class ReferenceAstroObject(ra: Double, dec: Double, errA: Double, errB: Double, mag: Double, magErr: Double, epoch: Double) {

  def toArray: Array[Double] = Array[Double](ra, dec, errA, errB, mag, magErr, epoch)

  override def toString: String = s"($ra, $dec, $errA, $errB, $mag, $magErr, $epoch)"

}
