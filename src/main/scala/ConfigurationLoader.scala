
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.net.URI
import scala.util.Try

/** A singleton object that provides methods to load various configurations from application's configuration file.
 */
object ConfigurationLoader {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val methodName = "ConfigurationLoader"

  // ################### CONFIG LOADER ###################
  // Load configurations using typesafe Config
  private val config = ConfigFactory.load()
  logger.info(s"$methodName: Configuration set up successfully")

  // ################### ENVIRONMENT CHECKER ###################
  // Check if the application is being run on the AWS
  logger.info(s"$methodName: Checking if the application is running on the AWS")
  private val isOnAWS = Try {
    val uri = new URI("http://169.254.169.254/latest/meta-data/")
    val url = uri.toURL
    val connection = url.openConnection()
    connection.setConnectTimeout(1000) // set timeout to 5 seconds
    connection.setReadTimeout(1000) // set read timeout to 5 seconds

    val source = scala.io.Source.fromInputStream(connection.getInputStream)
    try {
      source.mkString.nonEmpty
    } finally {
      source.close()
    }
  }.getOrElse(false)
  logger.info(s"$methodName: Is the application running on the AWS? $isOnAWS")

  // Check if the application is being run on the AWS EMR
  logger.info(s"$methodName: Checking if the application is running on the AWS EMR")
  private val isOnEMR = Try {
    val uri = new URI("http://169.254.169.254/latest/user-data/")
    val url = uri.toURL
    val connection = url.openConnection()
    connection.setConnectTimeout(1000) // set timeout to 5 seconds
    connection.setReadTimeout(1000) // set read timeout to 5 seconds

    val source = scala.io.Source.fromInputStream(connection.getInputStream)
    try {
      val userData = source.mkString
      userData.contains("emr-apps")
    } finally {
      source.close()
    }
  }.getOrElse(false)
  logger.info(s"$methodName: Is the application running on the AWS EMR? $isOnEMR")

  private val environment = if (isOnAWS && isOnEMR) "cloud" else "local"

  logger.info(s"$methodName: Environment set to $environment")

  // ################### DEBUG CHECKER ###################
  // Determine if the application is running in debug mode
  private val debug = config.getBoolean("app.debug")
  logger.info(s"$methodName: Is the application running in debug mode? $debug")

  // ################### FINAL PATHS SETTING ###################
  // Load appropriate configuration section based on debug mode
  logger.info(
    s"$methodName: Loading appropriate configuration section based on debug and environment settings"
  )
  private val appConfig =
    if (debug && environment == "local") {
      config.getConfig("app.local.debug")
    } else if (!debug && environment == "local") {
      config.getConfig("app.local.release")
    } else if (environment == "cloud") {
      config.getConfig("app.cloud")
    } else {
      throw new IllegalArgumentException(
        "Invalid configuration or environment settings"
      )
    }
  logger.info(
    "Configuration is set to take the path from the following section: " + appConfig
      .root()
      .render()
  )

  // ################### COMMON CONFIG ###################
  // Retrieve common configurations that are not environment dependent
  def getNumRandomWalks: Int = config.getInt("app.common.NumRandomWalks")
  def getSimilarityThreshold: Double = config.getDouble("app.common.SimilarityThreshold")
  def getNumRandomWalkSteps: Int = config.getInt("app.common.NumRandomWalkSteps")

  def getC: Double = config.getDouble("app.common.C")
  def getMaxDepth: Int = config.getInt("app.common.MaxDepth")
  def getThreshold: Double = config.getDouble("app.common.Threshold")

  // ################### SPARK SESSION BUILDER ###################
  def createSparkSession(): SparkSession = {
    val builder = SparkSession.builder.appName(appConfig.getString("appName"))

    // Based on the environment, configure Spark
    environment match {
      case "local" =>
        builder.master(appConfig.getString("master"))
      case "cloud" =>
        builder.master(appConfig.getString("master"))
          .config("spark.executor.memory", appConfig.getString("executorMemory"))
          .config("spark.executor.cores", appConfig.getString("executorCores"))
          .config("spark.dynamicAllocation.enabled", appConfig.getString("dynamicAllocationEnabled"))
          .config("spark.submit.deployMode", appConfig.getString("deployMode"))
        // If there is a 'totalExecutorCores' setting, include it
        if (appConfig.hasPath("totalExecutorCores")) {
          builder.config("spark.cores.max", appConfig.getString("totalExecutorCores"))
        }
    }
    builder.getOrCreate()
  }

  // ################### FILE PATH RETRIEVER ###################
  def getOriginalGraphFilePath: String = {
    // If we're in debug mode or if there's no specific debug/release distinction,
    // simply return the 'originalGraphFilePath' from the respective environment.
    if (debug || !appConfig.hasPath("release.originalGraphFilePath")) {
      appConfig.getString("originalGraphFilePath")
    } else {
      appConfig.getString("release.originalGraphFilePath")
    }
  }

  def getPerturbedGraphFilePath: String = {
    // Similar to the original graph file path, check for debug mode or lack of specific paths
    if (debug || !appConfig.hasPath("release.perturbedGraphFilePath")) {
      appConfig.getString("perturbedGraphFilePath")
    } else {
      appConfig.getString("release.perturbedGraphFilePath")
    }
  }

  // ################### PATH LOADER ###################
  def getOriginalNodesPath: String = {
    if (debug || !appConfig.hasPath("release.originalNodesPath")) {
      appConfig.getString("originalNodesPath")
    } else {
      appConfig.getString("release.originalNodesPath")
    }
  }

  def getOriginalEdgesPath: String = {
    if (debug || !appConfig.hasPath("release.originalEdgesPath")) {
      appConfig.getString("originalEdgesPath")
    } else {
      appConfig.getString("release.originalEdgesPath")
    }
  }

  def getPerturbedNodesPath: String = {
    if (debug || !appConfig.hasPath("release.perturbedNodesPath")) {
      appConfig.getString("perturbedNodesPath")
    } else {
      appConfig.getString("release.perturbedNodesPath")
    }
  }

  def getPerturbedEdgesPath: String = {
    if (debug || !appConfig.hasPath("release.perturbedEdgesPath")) {
      appConfig.getString("perturbedEdgesPath")
    } else {
      appConfig.getString("release.perturbedEdgesPath")
    }
  }

  // Add a method or a value to get the output directory from the configuration
  def getOutputDirectory: String = {
    if (debug || !appConfig.hasPath("release.outputDirectory")) {
      appConfig.getString("outputDirectory")
    } else {
      appConfig.getString("release.outputDirectory")
    }
  }

  // Method to generate the file name using time and day
  def generateFileName(): String = {
    val currentTime = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("HHmmss_MMddyyyy")
    s"MITMStatistics_${currentTime.format(formatter)}"
  }

}
