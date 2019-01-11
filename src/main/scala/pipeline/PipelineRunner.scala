package pipeline

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import pipeline.Parameters._

object PipelineRunner extends App {
  private val log = Logger.getLogger(this.getClass)

  val spark = SparkSession
    .builder()
    .appName("AstroPipeline")
    .getOrCreate()
  val sc = spark.sparkContext

  log.info("Running AstroPipeline")

  val conf = new Configuration()
  val fs = FileSystem.get(new URI(ClusterUri), conf)
  fs.delete(new Path(s"$OutputPath"), true)
  fs.mkdirs(new Path(s"$OutputPath"))

  val rdd = sc.parallelize(fs.listStatus(new Path(DataPath)))
  rdd.map(status => status.getPath.toString)
    .filter(path => path.endsWith(DataSuffix))
    .map(path => {
      val filename = path.split("/").last
      val localConf = new Configuration()
      val localFS = FileSystem.get(new URI(ClusterUri), localConf)
      localFS.copyToLocalFile(new Path(path), new Path(s"$HomePath/$LocalInputPath/$filename"))
      new FitsWrapper(s"$HomePath/$LocalInputPath/$filename")
    })
    .filter(image => image.isValid)
    .foreach(PipelineSteps.run)

  log.info("Finished running pipeline job")
}
