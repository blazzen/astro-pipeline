package pipeline

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import pipeline.Parameters._
import pipeline.entities.ReferenceAstroObject.GaiaDr2
import pipeline.utils.AstroUtils

object PipelineRunnerExtracted {

  def main(args: Array[String]) {

    val log = Logger.getLogger(this.getClass)

    val spark = SparkSession
      .builder()
      .appName("AstroPipelineExtracted")
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

    val wcsDistance = (ra1: Double, dec1: Double, ra2: Double, dec2: Double) => AstroUtils.wcsDistance(ra1, dec1, ra2, dec2)
    val wcsDistanceUdf = udf(wcsDistance)

    val getAreaHealpixIds = (ra: Double, dec: Double, radius: Double) => AstroUtils.getAreaHealpixIds(ra, dec, radius)
    val getAreaHealpixIdsUdf = udf(getAreaHealpixIds)

    implicit val portableStreamEncoder: Encoder[PortableDataStream] = org.apache.spark.sql.Encoders.kryo[PortableDataStream]
    implicit val portableStreamTupleEncoder: Encoder[(String, PortableDataStream)] =
      Encoders.tuple[String, PortableDataStream](ExpressionEncoder(), Encoders.kryo[PortableDataStream])

    implicit val portableStreamTuple5Encoder: Encoder[(PortableDataStream, String, Double, Double, Double)] =
      Encoders.tuple[PortableDataStream, String, Double, Double, Double](
        Encoders.kryo[PortableDataStream], ExpressionEncoder(), Encoders.scalaDouble, Encoders.scalaDouble, Encoders.scalaDouble
      )

    log.info("Running AstroPipeline")

    val conf = new Configuration()
    val fs = FileSystem.get(new URI(ClusterUri), conf)
    fs.delete(new Path(OutputPath), true)
    fs.mkdirs(new Path(OutputPath))

    val imagesDf = sc.binaryFiles(CatalogOutputPath, MinRddPartitions)
      .map(catalog => {
        val tokens = catalog._1.split("#")
        tokens(0) = tokens(0).split("/").last
        (catalog._2, tokens(0), tokens(1).toDouble, tokens(2).toDouble, tokens(3).split(".ldac")(0).toDouble)
      })
      .toDF("stream", "path", "centerRa", "centerDec", "radius")
      .withColumn("radius", $"radius" + PositionErrorDeg)

    imagesDf.persist()

    val boundsDf = spark.read.parquet(BoundsDfPath)

    val imagesWithPids = imagesDf.withColumn("hids", getAreaHealpixIdsUdf($"centerRa", $"centerDec", $"radius"))
      .select(explode($"hids").alias("hid"), $"stream", $"path", $"centerRa", $"centerDec", $"radius")
      .join(broadcast(boundsDf), $"hid" >= $"first" && $"hid" <= $"last")
      .select("stream", "path", "centerRa", "centerDec", "radius", "pid")
      .distinct

    imagesWithPids.persist()

    val pids = imagesWithPids.select($"pid")
      .distinct
      .as[Int]
      .collect

    println(s"partitions: ${pids.sorted.mkString(", ")}")
    println(s"count: ${pids.length}")

    val referenceDf = spark.read.parquet(ReferenceCatalogPath)
      .select("pid", GaiaDr2.Ra, GaiaDr2.Dec, GaiaDr2.RaError, GaiaDr2.DecError, GaiaDr2.RefEpoch, GaiaDr2.PhotGMeanFlux,
        GaiaDr2.PhotGMeanFluxError, GaiaDr2.PhotGMeanMag)

    referenceDf.filter($"pid".isin(pids: _*))
      .join(
        broadcast(imagesWithPids),
        imagesWithPids("pid") === referenceDf("pid") && wcsDistanceUdf($"centerRa", $"centerDec", $"ra", $"dec") < $"radius"
      ).groupBy("path")
      .agg(twoDimSeqAggregator)
      .join(imagesDf.select("path", "stream"), "path")
      .as[(String, Seq[Seq[Double]], PortableDataStream)](
      Encoders.tuple[String, Seq[Seq[Double]], PortableDataStream](ExpressionEncoder(), ExpressionEncoder(), portableStreamEncoder))
      .map(params => {
        PipelineSteps.runExtracted(params._1, params._2, params._3)
      })
      .foreach(_ => ())

    log.info("Finished running pipeline job")

  }
}
