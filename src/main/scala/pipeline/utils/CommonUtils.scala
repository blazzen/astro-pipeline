package pipeline.utils

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import org.apache.spark.input.PortableDataStream

import scala.io.Source

object CommonUtils {

  def listDir(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) d.listFiles.filter(_.isFile).map(_.toString).toList else List[String]()
  }

  def createDir(dir: String): Unit =
    new File(dir).mkdirs()

  def writeFileFromPortableStream(stream: PortableDataStream, destination: String): Unit =
    writeByteArray(stream.toArray(), destination)

  def writeByteArray(arr: Array[Byte], destination: String): Unit =
    Files.write(Paths.get(destination), arr)

  def verboseProcess(process: Process, log: Logger): Unit = {
    log.info("INPUT STREAM:")
    log.info(Source.fromInputStream(process.getInputStream).mkString)
    log.info("ERROR STREAM:")
    log.info(Source.fromInputStream(process.getErrorStream).mkString)
  }

}
