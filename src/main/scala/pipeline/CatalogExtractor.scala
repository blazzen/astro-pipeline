package pipeline

import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import pipeline.Parameters._
import pipeline.entities.FitsWrapper
import pipeline.utils.CommonUtils

object CatalogExtractor {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("CatalogExtractor")
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val conf = new Configuration()
    val fs = FileSystem.get(new URI(ClusterUri), conf)
    fs.delete(new Path(CatalogOutputPath), true)
    fs.mkdirs(new Path(CatalogOutputPath))

    val imagesRdd = sc.binaryFiles(DataPath, MinRddPartitions)
    imagesRdd
      .foreach(image => {
        val filename = image._1.split("/").last
        val destinationDir = s"$HomePath/$LocalInputPath"

        if (!Files.exists(Paths.get(destinationDir))) {
          CommonUtils.createDir(destinationDir)
        }
        val imagePath = s"$destinationDir/$filename"
        if (!Files.exists(Paths.get(imagePath))) {
          CommonUtils.writeFileFromPortableStream(image._2, imagePath)
        }
        val wrapper = new FitsWrapper(imagePath)
        val centerCoords = wrapper.centerWcsCoords
        val catalogName = s"${filename.split(DataSuffix)(0)}#${centerCoords._1}#${centerCoords._2}#${wrapper.radius}.ldac"

        val SextractorConfigPath = "conf/sextractor.config"
        val exec: Process = Runtime.getRuntime.exec(Array[String](
          "sextractor", "-c", s"$HomePath/$SextractorConfigPath", "-CATALOG_NAME", s"$destinationDir/$catalogName", imagePath
        ))
        exec.waitFor()
        val conf = new Configuration()
        val fs = FileSystem.get(new URI(ClusterUri), conf)
        PipelineSteps.moveToHDFS(s"$destinationDir/$catalogName", CatalogOutputPath, fs)
      })
  }
}
