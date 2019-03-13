package pipeline.utils

import java.io.File

object CommonUtils {

  def listDir(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) d.listFiles.filter(_.isFile).map(_.toString).toList else List[String]()
  }

  def createDir(dir: String): Unit = {
    new File(dir).mkdirs()
  }

}
