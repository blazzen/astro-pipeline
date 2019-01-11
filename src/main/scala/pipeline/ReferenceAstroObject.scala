package pipeline

import pipeline.utils.CommonUtils.PipelineConfigurationException

object ReferenceAstroObject {
  private val ReferenceCatalogName = "GAIA-DR2"

  private val Deg = math.Pi / 180.0
  private val Arcsec = Deg / 3600.0
  private val Mas = Arcsec / 1000.0

  //TODO: the order is different now, the mapping should be changed
  private val GaiaDr2Indices = Map(
    "ra" -> 5,
    "dec" -> 7,
    "ra_error" -> 6,
    "dec_error" -> 8,
    "ref_epoch" -> 4,
    "phot_g_mean_flux" -> 47,
    "phot_g_mean_flux_error" -> 48,
    "phot_g_mean_mag" -> 50
  )

  def fromGaiaDr2(line: String): ReferenceAstroObject = {
    val columns = line.split(",", -1).map(_.trim)
    val filteredColumns = GaiaDr2Indices.map(entry => (entry._1, columns(entry._2).toDouble))

    val flux = filteredColumns("phot_g_mean_flux")
    val fluxerr = filteredColumns("phot_g_mean_flux_error")
    var mag = filteredColumns("phot_g_mean_mag")
    var magerr = 1.0857 * fluxerr / flux
    if (flux <= 0.0) {
      mag = 99.0
      magerr = 99.0
    }

    ReferenceAstroObject(
      filteredColumns("ra"),
      filteredColumns("dec"),
      filteredColumns("ra_error") * Mas / Deg,
      filteredColumns("dec_error") * Mas / Deg,
      mag,
      magerr,
      filteredColumns("ref_epoch")
    )
  }

  def apply(line: String): ReferenceAstroObject = {
    ReferenceCatalogName match {
      case "GAIA-DR2" => fromGaiaDr2(line)
      case _ => throw new PipelineConfigurationException("Unknown reference catalog!")
    }
  }
}

case class ReferenceAstroObject(ra: Double, dec: Double, errA: Double, errB: Double, mag: Double, magErr: Double, epoch: Double) {

  def toArray: Array[Double] = Array[Double](ra, dec, errA, errB, mag, magErr, epoch)

  override def toString: String = s"($ra, $dec, $errA, $errB, $mag, $magErr, $epoch)"

}
