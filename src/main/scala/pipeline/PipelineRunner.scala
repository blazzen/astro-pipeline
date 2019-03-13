package pipeline

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import pipeline.Parameters._
import pipeline.ReferenceAstroObject.GaiaDr2
import pipeline.utils.AstroUtils

object PipelineRunner extends App {
  private val log = Logger.getLogger(this.getClass)

  val DistanceColumn = "distance"
  val FixedDistanceColumn = "fixed_distance"

  val spark = SparkSession
    .builder()
    .appName("AstroPipeline")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  log.info("Running AstroPipeline")

  val conf = new Configuration()
  val fs = FileSystem.get(new URI(ClusterUri), conf)
  fs.delete(new Path(OutputPath), true)
  fs.mkdirs(new Path(OutputPath))

  val referenceDf = spark.read.parquet(ReferenceCatalogPath)
    .select(GaiaDr2.Ra, GaiaDr2.Dec, GaiaDr2.RaError, GaiaDr2.DecError, GaiaDr2.RefEpoch, GaiaDr2.PhotGMeanFlux,
      GaiaDr2.PhotGMeanFluxError, GaiaDr2.PhotGMeanFluxError)
  referenceDf.persist()

  sc.parallelize(fs.listStatus(new Path(DataPath)))
    .map(status => status.getPath.toString)
    .filter(path => path.endsWith(DataSuffix))
    .map(path => {
      val filename = path.split("/").last
      val localConf = new Configuration()
      val localFS = FileSystem.get(new URI(ClusterUri), localConf)
      localFS.copyToLocalFile(new Path(path), new Path(s"$HomePath/$LocalInputPath/$filename"))
      (path, new FitsWrapper(s"$HomePath/$LocalInputPath/$filename"))
    })
    .filter {
      case (_, wrapper) => wrapper.isValid
    }
    .map {
      case (path, wrapper) => (path, wrapper.crval(1), wrapper.crval(2), wrapper.radius)
    }
    .toDF("path", "ra", "dec", "radius")
    .map {
      case Row(path: String, centerRa: Double, centerDec: Double, radius: Double) =>

        val centerRaDeg = centerRa * AstroUtils.Deg
        val centerDecDeg = centerDec * AstroUtils.Deg

        val centerDecDegSin = math.sin(centerDecDeg)
        val centerDecDegCos = math.cos(centerDecDeg)

        (
          path,
          referenceDf
            .withColumn(
              DistanceColumn,
              sin(col(GaiaDr2.Dec) * AstroUtils.Deg) * centerDecDegSin
                + cos(col(GaiaDr2.Dec) * AstroUtils.Deg) * centerDecDegCos * cos(col(GaiaDr2.Ra) * AstroUtils.Deg - centerRaDeg))
            .withColumn(
              FixedDistanceColumn,
              when(col(DistanceColumn) > -1.0,
                when(col(DistanceColumn) < 1.0, acos(col(DistanceColumn)) / AstroUtils.Deg)
                  .otherwise(0.0)
              ).otherwise(AstroUtils.PiDeg))
            .filter(col(FixedDistanceColumn) < radius)
            .drop(DistanceColumn, FixedDistanceColumn)
            .collect
            .map(_.toSeq.toArray.map(_.toString.toDouble))
        )
    }
    .foreach(args => PipelineSteps.run(args._1, args._2))

  log.info("Finished running pipeline job")
}
