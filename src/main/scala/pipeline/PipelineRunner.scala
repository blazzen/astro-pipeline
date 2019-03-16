package pipeline

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.functions._
import pipeline.Parameters._
import pipeline.ReferenceAstroObject.GaiaDr2
import pipeline.utils.AstroUtils

object PipelineRunner extends App {
  private val log = Logger.getLogger(this.getClass)

  val PathColumn = "path"
  val CenterRaColumn = "centerRa"
  val CenterDecColumn = "centerDec"
  val RadiusColumn = "radius"

  val spark = SparkSession
    .builder()
    .appName("AstroPipeline")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  val twoDimSeqAggregator = new Aggregator[Row, Seq[Seq[Double]], Seq[Seq[Double]]] with Serializable {
    def zero: Seq[Seq[Double]] =
      Seq[Seq[Double]]()

    def reduce(buf: Seq[Seq[Double]], row: Row): Seq[Seq[Double]] =
      buf :+ GaiaDr2.Columns.map(column => row.getAs[Double](column))

    def merge(buf1: Seq[Seq[Double]], buf2: Seq[Seq[Double]]): Seq[Seq[Double]] =
      buf1 ++ buf2

    def finish(buf: Seq[Seq[Double]]): Seq[Seq[Double]] =
      buf

    def bufferEncoder: Encoder[Seq[Seq[Double]]] = ExpressionEncoder()

    def outputEncoder: Encoder[Seq[Seq[Double]]] = ExpressionEncoder()
  }.toColumn

  log.info("Running AstroPipeline")

  val conf = new Configuration()
  val fs = FileSystem.get(new URI(ClusterUri), conf)
  fs.delete(new Path(OutputPath), true)
  fs.mkdirs(new Path(OutputPath))

  val referenceDf = spark.read.parquet(ReferenceCatalogPath)
    .select(GaiaDr2.Ra, GaiaDr2.Dec, GaiaDr2.RaError, GaiaDr2.DecError, GaiaDr2.RefEpoch, GaiaDr2.PhotGMeanFlux,
      GaiaDr2.PhotGMeanFluxError, GaiaDr2.PhotGMeanFluxError)

  val images = sc.parallelize(fs.listStatus(new Path(DataPath)))
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
    .toDF(PathColumn, CenterRaColumn, CenterDecColumn, RadiusColumn)
    .crossJoin(referenceDf)
    .filter(row => {
      val centerRaDeg = row.getAs[Double](CenterRaColumn) * AstroUtils.Deg
      val centerDecDeg = row.getAs[Double](CenterDecColumn) * AstroUtils.Deg

      val centerDecDegSin = math.sin(centerDecDeg)
      val centerDecDegCos = math.cos(centerDecDeg)

      val currentRaDeg = row.getAs[Double](GaiaDr2.Ra) * AstroUtils.Deg
      val currentDecDeg = row.getAs[Double](GaiaDr2.Dec) * AstroUtils.Deg

      var distance = math.sin(currentDecDeg) * centerDecDegSin +
        math.cos(currentDecDeg) * centerDecDegCos * math.cos(currentRaDeg - centerRaDeg)

      distance =
        if (distance > -1.0) {
          if (distance < 1.0) math.acos(distance) / AstroUtils.Deg else 0.0
        } else {
          AstroUtils.PiDeg
        }

      distance < row.getAs[Double](RadiusColumn)
    })
    .drop(CenterRaColumn, CenterDecColumn, RadiusColumn)
    .groupBy(PathColumn)
    .agg(twoDimSeqAggregator)
    .foreach(row => PipelineSteps.run(row.getString(0), row.getSeq[Seq[Double]](1)))

  log.info("Finished running pipeline job")
}
