package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import pipeline.utils.AstroUtils
import pipeline.Parameters._

object Partitioner {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Partitioner")
      .getOrCreate()

    import spark.implicits._

    val getHealpixId = (ra: Double, dec: Double) => AstroUtils.getHealpixId(ra, dec)
    val getHealpixIdUdf = udf(getHealpixId)

    val numResultPartitions = 1024
    val shufflePartitions = spark.conf.get("spark.sql.shuffle.partitions")

    val df = spark.read.parquet(ReferenceCatalogPath)
      .select("ra", "dec", "ra_error", "dec_error", "ref_epoch", "phot_g_mean_flux", "phot_g_mean_flux_error", "phot_g_mean_mag")
      .withColumn("hid", getHealpixIdUdf($"ra", $"dec"))

    spark.conf.set("spark.sql.shuffle.partitions", numResultPartitions)

    df.orderBy($"hid")
      .withColumn("pid", spark_partition_id())
      .write
      .partitionBy("pid")
//      .bucketBy(10, "hid")
      .option("path", "wasb:///gaia/gaia_table_1024x10")
      .saveAsTable("gaia2_1024x10")

    spark.conf.set("spark.sql.shuffle.partitions", shufflePartitions)

    spark.table("gaia2_1024x10")
      .groupBy("pid")
      .agg(min($"hid").alias("first"), max($"hid").alias("last"))
      .orderBy($"pid")
      .repartition(16)
      .write.parquet("wasb:///gaia/bounds_1024x10.parquet")
  }
}
