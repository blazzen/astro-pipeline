package pipeline

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import pipeline.utils.CommonUtils
import pipeline.Parameters._

object PipelineSteps {
  private val log = Logger.getLogger(this.getClass)

  private val PureNamePattern = ".*\\/(.*)\\.fits".r
  //should be configurable as well
  private val SextractorConfigPath = "conf/sextractor.config"
  private val ScampConfigPath = "conf/scamp.config"
  private val ExtractedCatalogName = "extracted.ldac"
  private val ExtractionLogName = "extraction_log.txt"
  private val CalibratedHeadName = "extracted.head"
  private val CalibrationLogName = "calibration_log.txt"

  def run(image: FitsWrapper): Unit = {
    val PureNamePattern(destinationDir) = image.filename
    val destinationPath = s"$HomePath/$LocalOutputPath/$destinationDir"
    extract(image)
    if (Files.exists(Paths.get(s"$destinationPath/$ExtractedCatalogName"))) {
      calibrate(image)
      if (Files.exists(Paths.get(s"$destinationPath/$CalibratedHeadName"))) {
        moveToHDFS(s"$destinationPath", s"$OutputPath")
        image.calculateRadius()
        return
      }
    }

    //cleanup in case of errors during pipeline steps
    FileUtils.deleteQuietly(new File(s"$destinationPath"))
    FileUtils.deleteQuietly(new File(s"${image.filename}"))
  }

  def extract(fitsWrapper: FitsWrapper): Unit = {
    val PureNamePattern(destinationDir) = fitsWrapper.filename
    val destinationPath = s"$HomePath/$LocalOutputPath/$destinationDir"
    val catalogPath = s"$destinationPath/$ExtractedCatalogName"
    val logPath = s"$destinationPath/$ExtractionLogName"

    CommonUtils.createDir(destinationPath)

    log.info(s"Extracting image: ${fitsWrapper.filename}")

    val exec: Process = Runtime.getRuntime.exec(Array[String](
      "sextractor", "-c", s"$HomePath/$SextractorConfigPath", "-CATALOG_NAME", s"$catalogPath", s"${fitsWrapper.filename}"
    ))
    //for some reason SExtractor writes it's output to stderr
    val stderrStream = exec.getErrorStream
    FileUtils.copyInputStreamToFile(stderrStream, new File(logPath))

    if (Files.exists(Paths.get(s"$destinationPath/$ExtractedCatalogName"))) {
      log.info(s"Extracted ${fitsWrapper.filename} to $catalogPath")
    } else {
      log.error(s"Failed to extract ${fitsWrapper.filename} to $catalogPath")
    }
  }

  def calibrate(fitsWrapper: FitsWrapper): Unit = {
    val PureNamePattern(destinationDir) = fitsWrapper.filename
    val destinationPath = s"$HomePath/$LocalOutputPath/$destinationDir"
    val logPath = s"$destinationPath/$CalibrationLogName"

    log.info(s"Calibrating image ${fitsWrapper.filename}")

    val exec: Process = Runtime.getRuntime.exec(Array[String](
      "scamp", "-c", s"$HomePath/$ScampConfigPath", "-REFOUT_CATPATH", s"$destinationPath", s"$destinationPath/$ExtractedCatalogName"
    ))
    val stderrStream = exec.getErrorStream
    FileUtils.copyInputStreamToFile(stderrStream, new File(logPath))

    if (Files.exists(Paths.get(s"$destinationPath/$CalibratedHeadName"))) {
      log.info(s"Calibrated ${fitsWrapper.filename}")
    } else {
      log.error(s"Failed to calibrate ${fitsWrapper.filename}")
    }
  }

  def moveToHDFS(from: String, to: String): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(new URI(ClusterUri), conf)
    fs.moveFromLocalFile(new Path(from), new Path(to))
    log.info(s"Moved $from to HDFS dir $to")
  }
}
