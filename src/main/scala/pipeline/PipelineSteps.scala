package pipeline

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import pipeline.utils.{CommonUtils, FitsLdacBuilder}
import pipeline.Parameters._

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

  def run(path: String, objects: Seq[Seq[Double]]): Unit = {
    val filename = path.split("/").last
    val destinationDir = filename.split(DataSuffix)(0)
    val destinationPath = s"$HomePath/$LocalOutputPath/$destinationDir"
    val localImagePath = s"$HomePath/$LocalInputPath/$filename"

    val conf = new Configuration()
    val fs = FileSystem.get(new URI(ClusterUri), conf)
    if (!Files.exists(Paths.get(localImagePath))) {
      fs.copyToLocalFile(new Path(path), new Path(localImagePath))
    }

    val localRefCatalogPath = s"$destinationPath/astrefcat.cat"
    FitsLdacBuilder.fromSeq(objects, localRefCatalogPath)

    if (!Files.exists(Paths.get(localRefCatalogPath))) {
      extract(localImagePath, destinationPath)
      if (Files.exists(Paths.get(s"$destinationPath/$ExtractedCatalogName"))) {
        calibrate(localImagePath, localRefCatalogPath, destinationPath)
        if (Files.exists(Paths.get(s"$destinationPath/$CalibratedHeadName"))) {
          moveToHDFS(destinationPath, OutputPath, fs)
          return
        }
      }
    } else {
      log.error(s"$localRefCatalogPath wasn't created")
    }

    //cleanup in case of errors during pipeline steps
    FileUtils.deleteQuietly(new File(destinationPath))
    FileUtils.deleteQuietly(new File(localImagePath))
    FileUtils.deleteQuietly(new File(localRefCatalogPath))
  }

  def extract(imagePath: String, destinationPath: String): Unit = {
    val catalogPath = s"$destinationPath/$ExtractedCatalogName"
    val logPath = s"$destinationPath/$ExtractionLogName"

    CommonUtils.createDir(destinationPath)

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

  def calibrate(imagePath: String, localRefCatalogPath: String, destinationPath: String): Unit = {
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

    if (Files.exists(Paths.get(s"$destinationPath/$CalibratedHeadName"))) {
      log.info(s"Calibrated $imagePath")
    } else {
      log.error(s"Failed to calibrate $imagePath")
    }
  }

  def moveToHDFS(from: String, to: String, fs: FileSystem): Unit = {
    fs.moveFromLocalFile(new Path(from), new Path(to))
    log.info(s"Moved $from to HDFS dir $to")
  }
}
