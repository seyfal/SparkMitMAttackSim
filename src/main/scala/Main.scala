package com.lsc

import NetGraphAlgebraDefs.NodeObject
import org.slf4j.LoggerFactory
import com.lsc.LoadMethods.{deserializeWithCustomClassLoader, saveToGraphX}
import com.lsc.AttackSimulation.evaluateRandomWalkAndDecide
import com.lsc.ConfigurationLoader.{generateFileName, getNumRandomWalkSteps, getNumRandomWalks, getOriginalGraphFilePath, getOutputDirectory, getPerturbedGraphFilePath, getSimilarityThreshold}
import com.lsc.Statistics.setNumberOfNodes

import java.io.FileInputStream

object Main {

  // Initialize the logger for the application
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val methodName = "Main"

  def main(args: Array[String]): Unit = {
    // Begin try-catch block for exception handling
    try {
      // Log the start of the main function
      logger.info(s"$methodName: Starting the program")

      // Initialize SparkSession with settings from ConfigurationLoader
      val spark = ConfigurationLoader.createSparkSession()
      logger.debug(s"$methodName: SparkSession initialized")

      // Extract SparkContext from SparkSession
      val sc = spark.sparkContext
      logger.debug(s"$methodName: SparkContext extracted from SparkSession")

      // Deserialize a previously saved graph from file
      val originalGraphDeserialized: List[NodeObject] = deserializeWithCustomClassLoader[List[NodeObject]](
        new FileInputStream(getOriginalGraphFilePath)
      )

      val perturbedGraphDeserialized: List[NodeObject] = deserializeWithCustomClassLoader[List[NodeObject]](
        new FileInputStream(getPerturbedGraphFilePath)
      )

      // Convert the deserialized list into a GraphX graph
      val originalGraphRDD = saveToGraphX(originalGraphDeserialized, sc)
      val perturbedGraphRDD = saveToGraphX(perturbedGraphDeserialized, sc)

      // set the size of the graph
      setNumberOfNodes(originalGraphRDD.numVertices)

      // Log the completion of the graph loading
      logger.debug(s"$methodName: Graph loaded successfully")

      // Evaluate the random walk attack simulation
      evaluateRandomWalkAndDecide(sc, originalGraphRDD, perturbedGraphRDD, getNumRandomWalks, getNumRandomWalkSteps, getSimilarityThreshold)

      // Generate and print a summary of attack statistics
      val summary = Statistics.generateSummary()
      logger.info(s"$methodName: Summary of attack statistics:\n$summary")

      // Assuming the output directory and file name are given
      val outputDirectory = getOutputDirectory
      val fileName = generateFileName() + ".yaml"

      // Save the YAML summary
      Statistics.saveSummaryToFile(outputDirectory, fileName)

      // Stop the SparkSession
      spark.stop()
      logger.info(s"$methodName: Program finished successfully")
    } catch {
      // Log exceptions at the error level and rethrow to handle upstream if necessary
      case e: Exception =>
        logger.error(s"$methodName: An exception occurred:", e)
        throw e // Optionally rethrow if you want to propagate the error
    } finally {
      // Finally block to execute clean-up actions or logging that should occur regardless of success or failure
      logger.debug(s"$methodName: Exiting the program")
    }
  }
}
