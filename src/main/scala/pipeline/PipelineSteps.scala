package pipeline

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.input.PortableDataStream
import pipeline.utils.{CommonUtils, FitsLdacBuilder}
import pipeline.Parameters._
import pipeline.entities.ReferenceAstroObject

import scala.io.Source

object PipelineSteps {
  private val log = Logger.getLogger(this.getClass)

  private val PureNamePattern = ".*\\/(.*?)\\.fits".r
  // should be configurable as well
  private val SextractorConfigPath = "conf/sextractor.config"
  private val ScampConfigPath = "conf/scamp.config"
  private val ExtractedCatalogName = "extracted.ldac"
  private val ExtractionLogName = "extraction_log.txt"
  private val CalibratedHeadName = "extracted.head"
  private val CalibrationLogName = "calibration_log.txt"

  case class CalibrationResult(path: String, crval1: Double, crval2: Double, astrrms1: Double, astrrms2: Double) {
    override def toString: String = s"($path, $crval1, $crval2, $astrrms1, $astrrms2)"
  }

  def run(path: String, objects: Seq[Seq[Double]], bytes: Array[Byte]): CalibrationResult = {
    val filename = path.split("/").last
    val localImagePath = s"$HomePath/$LocalInputPath/$filename"

    if (!Files.exists(Paths.get(localImagePath))) {
      log.info(s"Creating file $localImagePath from wrapper")

      CommonUtils.writeByteArray(bytes, localImagePath)
    } else {
      log.info(s"$localImagePath has been already created")
    }

    run(path, objects.toArray.map(ReferenceAstroObject(_)))
  }

  def runExtracted(path: String, objects: Seq[Seq[Double]], stream: PortableDataStream): CalibrationResult = {
    val resultDir = s"$HomePath/$LocalOutputPath"
    val localCatalogPath = s"$resultDir/$path.ldac"
    val localRefCatalogPath = s"$resultDir/$path.cat"

    if (!Files.exists(Paths.get(resultDir))) {
      CommonUtils.createDir(resultDir)
    }

    CommonUtils.writeFileFromPortableStream(stream, localCatalogPath)
    if (objects.nonEmpty) {
      FitsLdacBuilder.fromSeq(objects, localRefCatalogPath)
    }

    calibrateExtracted(s"$path.fits", localCatalogPath, localRefCatalogPath)
  }

  def calibrateExtracted(
                          image: String,
                          localCatalogPath: String,
                          localRefCatalogPath: String
                        ): CalibrationResult = {


    log.info(s"Calibrating image $image")

    val exec: Process = Runtime.getRuntime.exec(Array[String](
      "scamp",
      "-c", s"$HomePath/$ScampConfigPath",
      "-ASTREF_CATALOG", "FILE",
      "-ASTREFCAT_NAME", localRefCatalogPath,
      localCatalogPath
    ))
    exec.waitFor()

    val crval = Array(-1.0, -1.0)
    val astrrms = Array(-1.0, -1.0)
    val resultPath = localCatalogPath.replaceAllLiterally(".ldac", ".head")
    if (Files.exists(Paths.get(s"$resultPath"))) {
      log.info(s"Successfully calibrated $image")
      val crvalRegex = "CRVAL([1,2])\\W*=\\W*(.+)\\/\\W*World.+".r
      val astrrmsRegex = "ASTRRMS([1,2])\\W*=\\W*(.+)\\/\\W*Astrom.+".r
      Source.fromFile(s"$resultPath").getLines.foreach {
        case crvalRegex(i, value) => crval(i.toInt - 1) = value.toDouble
        case astrrmsRegex(i, value) => astrrms(i.toInt - 1) = value.toDouble
        case _ =>
      }
    } else {
      log.error(s"Failed to calibrate $image")
    }

    CalibrationResult(image, crval(0), crval(1), astrrms(0), astrrms(1))
  }

  def run(path: String, objects: Seq[Seq[Double]]): CalibrationResult = {
    run(path, objects.toArray.map(ReferenceAstroObject(_)))
  }

  def run(path: String, objects: Array[ReferenceAstroObject]): CalibrationResult = {
    if (objects != null) {
      log.info(s"Got  ${objects.length} reference rows")
    } else {
      throw new RuntimeException("The objects param is null!")
    }

    val filename = path.split("/").last
    val destinationDir = filename.split(DataSuffix)(0)
    val destinationPath = s"$HomePath/$LocalOutputPath/$destinationDir"
    val localImagePath = s"$HomePath/$LocalInputPath/$filename"

    FileUtils.deleteQuietly(new File(destinationPath))
    CommonUtils.createDir(destinationPath)
    if (!Files.exists(Paths.get(destinationPath))) {
      throw new RuntimeException(s"$destinationPath wasn't created!")
    }
    if (!Files.exists(Paths.get(localImagePath))) {
      throw new RuntimeException(s"$localImagePath not downloaded")
    }

    val localRefCatalogPath = s"$destinationPath/astrefcat.cat"
    if (objects.nonEmpty) {
      FitsLdacBuilder.fromObjectsArray(objects, localRefCatalogPath)
    }

    if (Files.exists(Paths.get(localRefCatalogPath))) {
      extract(localImagePath, destinationPath)
      if (Files.exists(Paths.get(s"$destinationPath/$ExtractedCatalogName"))) {
        return calibrate(localImagePath, localRefCatalogPath, destinationPath, removeSources = true)
      }
    } else {
      log.error(s"$localRefCatalogPath wasn't created")
    }

    //cleanup in case of errors during pipeline steps
    FileUtils.deleteQuietly(new File(destinationPath))
    FileUtils.deleteQuietly(new File(localImagePath))
    CalibrationResult(path, -1, -1, -1, -1)
  }

  def extract(imagePath: String, destinationPath: String): Unit = {
    val catalogPath = s"$destinationPath/$ExtractedCatalogName"
    val logPath = s"$destinationPath/$ExtractionLogName"

    log.info(s"Extracting image: $imagePath")

    val exec: Process = Runtime.getRuntime.exec(Array[String](
      "sextractor", "-c", s"$HomePath/$SextractorConfigPath", "-CATALOG_NAME", catalogPath, imagePath
    ))

    //for some reason SExtractor writes it's output to stderr
    val stderrStream = exec.getErrorStream
    FileUtils.copyInputStreamToFile(stderrStream, new File(logPath))

    if (Files.exists(Paths.get(s"$destinationPath/$ExtractedCatalogName"))) {
      log.info(s"Extracted $imagePath to $catalogPath")
    } else {
      log.error(s"Failed to extract $imagePath to $catalogPath")
    }
  }

  def calibrate(imagePath: String, localRefCatalogPath: String, destinationPath: String, removeSources: Boolean): CalibrationResult = {
    val logPath = s"$destinationPath/$CalibrationLogName"

    log.info(s"Calibrating image $imagePath")

    val exec: Process = Runtime.getRuntime.exec(Array[String](
      "scamp",
      "-c", s"$HomePath/$ScampConfigPath",
      "-REFOUT_CATPATH", destinationPath,
      "-ASTREF_CATALOG", "FILE",
      "-ASTREFCAT_NAME", localRefCatalogPath,
      s"$destinationPath/$ExtractedCatalogName"
    ))
    val stderrStream = exec.getErrorStream
    FileUtils.copyInputStreamToFile(stderrStream, new File(logPath))

    val crval = Array(0.0, 0.0)
    val astrrms = Array(0.0, 0.0)
    if (Files.exists(Paths.get(s"$destinationPath/$CalibratedHeadName"))) {
      log.info(s"Calibrated $imagePath")
      val crvalRegex = "CRVAL([1,2])\\W*=\\W*(.+)\\/\\W*World.+".r
      val astrrmsRegex = "ASTRRMS([1,2])\\W*=\\W*(.+)\\/\\W*Astrom.+".r
      Source.fromFile(s"$destinationPath/$CalibratedHeadName").getLines.foreach {
        case crvalRegex(i, value) => crval(i.toInt - 1) = value.toDouble
        case astrrmsRegex(i, value) => astrrms(i.toInt - 1) = value.toDouble
        case _ =>
      }
    } else {
      log.error(s"Failed to calibrate $imagePath")
    }

    if (removeSources) {
      FileUtils.deleteQuietly(new File(destinationPath))
      FileUtils.deleteQuietly(new File(imagePath))
    }

    CalibrationResult(imagePath.split("/").last, crval(0), crval(1), astrrms(0), astrrms(1))
  }

  def moveToHDFS(from: String, to: String, fs: FileSystem): Unit = {
    fs.moveFromLocalFile(new Path(from), new Path(to))
    log.info(s"Moved $from to HDFS dir $to")
  }
}
