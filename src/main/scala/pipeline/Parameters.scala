package pipeline

// the convenient way to configure the pipeline is setting properties,
// but there is a problem with packing it into jar and sending to cluster, so I decided to create this object with constants

object Parameters {
  // hidden for security purposes
  val ClusterUri = ""

  val DataPath = "wasb:///pipeline/wise-600"
  val OutputPath = "wasb:///pipeline/output"
  val CatalogOutputPath = "wasb:///pipeline/wise-600-catalogs"
  val ReferenceCatalogPath = "wasb:///gaia/gaia_table_1024x10"
  val BoundsDfPath = "wasb:///gaia/bounds_1024x10.parquet"
  val HomePath = "/home/sshuser"
  val LocalInputPath = "input"
  val LocalOutputPath = "output"

  val DataSuffix = ".fits"

  val PositionErrorDeg: Double = 1.0 / 60.0
  val MinRddPartitions: Int = 55
}
