package pipeline.entities

import scala.io.Source

object ExtractedAstroObject {

  def fromCatalog(path: String): Array[ExtractedAstroObject] = {
    val source = Source.fromFile(path)
    val lineIterator = source.getLines
    var objects = Array[ExtractedAstroObject]()
    while (lineIterator.hasNext) {
      val line = lineIterator.next
      if (!line.startsWith("#")) {
        objects :+= ExtractedAstroObject(line)
      }
    }
    source.close()
    objects
  }

  def apply(s: String): ExtractedAstroObject = {
    val arr = s.split(" ").filter(!_.isEmpty).map(_.trim.toDouble)
    new ExtractedAstroObject(arr(0), arr(1), arr(2), arr(3), arr(4))
  }

}

case class ExtractedAstroObject(x: Double, y: Double, center_x: Double, center_y: Double, flux: Double) {

  override def toString: String = s"($x, $y, $center_x, $center_y, $flux)"

}
