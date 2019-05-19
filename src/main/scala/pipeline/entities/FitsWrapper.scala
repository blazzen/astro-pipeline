package pipeline.entities

import io.github.malapert.jwcs.JWcsFits
import nom.tam.fits._
import org.apache.log4j.Logger
import pipeline.utils.AstroUtils

class FitsWrapper(var filename: String) {

  private val log = Logger.getLogger(this.getClass)

  private val PrimaryExtensionIndex = 0

  private var image: Fits = _
  private var basicHDU: BasicHDU = _
  private var header: Header = _
  private var wcs: JWcsFits = _

  private var wcsCreated: Boolean = false
  private var wcsInitError: Boolean = false

  var wcsRadius: Double = -1.0

  try {
    image = new Fits(filename)
    basicHDU = image.getHDU(PrimaryExtensionIndex)
    header = if (basicHDU != null) basicHDU.getHeader else null
    wcs = new JWcsFits(image, PrimaryExtensionIndex)
    wcsCreated = true
    wcs.doInit()
  } catch {
    case e: Exception =>
      log.error(s"Error loading image $filename", e)
      if (wcsCreated) wcsInitError = true
  }


  def isValid: Boolean = {
    if (basicHDU == null) {
      log.error(s"Can't find primary HDU for $filename")

    } else if (header == null) {
      log.error(s"Can't find header for basic HDU of $filename")

    } else if (!header.containsKey("CTYPE1") || !header.containsKey("CTYPE2")) {
      log.error(s"No CTYPE* keyword for $filename")

    } else if (!header.containsKey("CRVAL1") || !header.containsKey("CRVAL2")) {
      log.error(s"No CRVAL* keyword for $filename")

    } else if (!header.containsKey("CRPIX1") || !header.containsKey("CRPIX2")) {
      log.error(s"No CRPIX* keyword for $filename")

    } else if (wcs == null) {
      log.error(s"Can't create object containing WCS info for $filename")

    } else if (wcsInitError) {
      log.error(s"Can't initialize WCS info for $filename")

    } else {
      return true
    }

    false
  }

  def ctype(i: Int): String = {
    require(i == 1 || i == 2, "invalid index")
    header.getStringValue(s"CTYPE$i").trim
  }

  def crval(i: Int): Double = {
    require(i == 1 || i == 2, "invalid index")
    header.getDoubleValue(s"CRVAL$i")
  }

  def centerWcsCoords: (Double, Double) = {
    val firstCenterPixCoord = (axisLen(1) + 1).toDouble / 2
    val secondCenterPixCoord = (axisLen(0) + 1).toDouble / 2
    val centerWcsCoords = wcs.pix2wcs(firstCenterPixCoord, secondCenterPixCoord)
    (centerWcsCoords(0), centerWcsCoords(1))
  }

  def crpix(i: Int): Double = {
    require(i == 1 || i == 2, "invalid index")
    header.getDoubleValue(s"CRPIX$i")
  }

  def axisLen(i: Int): Int = {
    require(i == 0 || i == 1, "invalid index")
    basicHDU.getAxes()(i)
  }

  def radius: Double = {
    if (wcsRadius == -1.0) {
      wcsRadius = calculateRadius
    }
    wcsRadius
  }

  def calculateRadius: Double = {
    val firstCenterPixCoord = (axisLen(1) + 1).toDouble / 2
    val secondCenterPixCoord = (axisLen(0) + 1).toDouble / 2
    val centerWcsCoords = wcs.pix2wcs(firstCenterPixCoord, secondCenterPixCoord)
    val cornerWcsCoords = wcs.pix2wcs(0, 0)

    AstroUtils.wcsDistance(centerWcsCoords, cornerWcsCoords)
  }
}
