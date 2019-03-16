package pipeline.utils

import nom.tam.fits._
import nom.tam.util.BufferedFile
import org.apache.log4j.Logger
import pipeline.ReferenceAstroObject

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object FitsLdacBuilder {
  private val log = Logger.getLogger(this.getClass)

  case class FitsColumnSpecification(ttype: String, form: String, unit: String, disp: String)

  private val FitsColumnsSpecifications = Seq(
    FitsColumnSpecification("X_WORLD", "1D", "deg", "E15"),
    FitsColumnSpecification("Y_WORLD", "1D", "deg", "E15"),
    FitsColumnSpecification("ERRA_WORLD", "1E", "deg", "E12"),
    FitsColumnSpecification("ERRB_WORLD", "1E", "deg", "E12"),
    FitsColumnSpecification("MAG", "1E", "mag", "F8.4"),
    FitsColumnSpecification("MAGERR", "1E", "mag", "F8.4"),
    FitsColumnSpecification("OBSDATE", "1D", "yr", "F13.8")
  )

  private def buildPrimaryHDU: BasicHDU = {
    val primaryHDU = FitsFactory.HDUFactory(Array[Double]())
    primaryHDU.getHeader.addValue("BITPIX", 8, "array data type")
    primaryHDU.getHeader.addValue("NAXIS", 0, "number of array dimensions")
    primaryHDU.getHeader.deleteKey("NAXIS1")
    primaryHDU.getHeader.addValue("EXTEND", true, "")
    primaryHDU
  }

  private def buildLdacImheadHDU: BasicHDU = {
    val fakeTable = Array.ofDim[String](1, 1)
    fakeTable(0)(0) = "I need to write something to this binary table becauseof strange FITS LDAC pseudo-standard."

    val fakeHeader = new Header()
    fakeHeader.addValue("XTENSION", "BINTABLE", "binary table extension")
    fakeHeader.addValue("BITPIX", 8, "array data type")
    fakeHeader.addValue("NAXIS", 2, "number of array dimensions")
    fakeHeader.addValue("NAXIS1", 2880, "length of dimension 1")
    fakeHeader.addValue("NAXIS2", 1, "length of dimension 2")
    fakeHeader.addValue("PCOUNT", 0, "number of group parameters")
    fakeHeader.addValue("GCOUNT", 1, "number of groups")
    fakeHeader.addValue("TFIELDS", 1, "number of table fields")
    fakeHeader.addValue("TTYPE1", "Field Header Card", "")
    fakeHeader.addValue("TFORM1", "2880A", "")
    fakeHeader.addValue("EXTNAME", "LDAC_IMHEAD", "extension name")
    fakeHeader.addValue("TDIM1", s"(80, ${fakeTable(0)(0).length})", "")

    FitsFactory.HDUFactory(fakeHeader, BinaryTableHDU.encapsulate(fakeTable))
  }

  private def buildLdacObjectsHDU(referenceObjects: Array[ReferenceAstroObject]): BasicHDU = {
    val transposed = referenceObjects.map(_.toArray).transpose
    val table = new BinaryTable()
    FitsColumnsSpecifications.zipWithIndex.foreach(indexedSpec => {
      if (indexedSpec._1.form.equals("1D")) {
        table.addColumn(transposed(indexedSpec._2))
      } else if (indexedSpec._1.form.equals("1E")) {
        table.addColumn(transposed(indexedSpec._2).map(_.toFloat))
      }
    })

    val header = BinaryTableHDU.manufactureHeader(table)
    header.addValue("XTENSION", "BINTABLE", "binary table extension")
    header.addValue("BITPIX", 8, "array data type")
    header.addValue("NAXIS", 2, "number of array dimensions")
    header.addValue("PCOUNT", 0, "number of group parameters")
    header.addValue("GCOUNT", 1, "number of groups")
    header.addValue("EXTNAME", "LDAC_OBJECTS", "extension name")

    FitsColumnsSpecifications.zip(Stream from 1).foreach(indexedSpec => {
      header.addValue(s"TTYPE${indexedSpec._2}", s"${indexedSpec._1.ttype}", "")
      header.addValue(s"TFORM${indexedSpec._2}", s"${indexedSpec._1.form}", "")
      header.addValue(s"TUNIT${indexedSpec._2}", s"${indexedSpec._1.unit}", "")
      header.addValue(s"TDISP${indexedSpec._2}", s"${indexedSpec._1.disp}", "")
    })

    FitsFactory.HDUFactory(header, table)
  }

  private def write(objects: Array[ReferenceAstroObject], destination: String): Unit = {
    val fitsLdac: Fits = new Fits()
    fitsLdac.addHDU(buildPrimaryHDU)
    fitsLdac.addHDU(buildLdacImheadHDU)
    fitsLdac.addHDU(buildLdacObjectsHDU(objects))
    fitsLdac.write(new BufferedFile(destination, "rw"))
  }

  def fromCsv(filename: String, destination: String): Unit = {
    val bufferedSource = Source.fromFile(filename)
    val rows = ArrayBuffer[ReferenceAstroObject]()

    for (line <- bufferedSource.getLines().drop(1)) {
      try {
        rows += ReferenceAstroObject(line)
      } catch {
        case e: Exception => log.error(s"Can't parse csv line: $line", e)
      }
    }

    bufferedSource.close

    write(rows.toArray, destination)
  }

  def fromArray(data: Array[Array[Double]], destination: String): Unit =
    write(data.map(params => ReferenceAstroObject(params)), destination)

  def fromSeq(data: Seq[Seq[Double]], destination: String): Unit =
    write(data.toArray.map(params => ReferenceAstroObject(params)), destination)
}
