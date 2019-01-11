package pipeline

//the convenient way to configure the pipeline is setting properties,
//but there is a problem with packing it into jar and sending to cluster, so I decided to create this object with constants

object Parameters {
  //hidden for security purposes
  val ClusterUri = ""

  val DataPath = "wasb:///pipeline/input"
  val OutputPath = "wasb:///pipeline/output"
  val HomePath = "/home/sshuser"
  val LocalInputPath = "input"
  val LocalOutputPath = "output"

  val DataSuffix = ".fits"
}
