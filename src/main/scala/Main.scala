/*
    Copyright (c) 2023 Seyfal Sultanov

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

import org.slf4j.LoggerFactory
import AttackSimulation.evaluateRandomWalkAndDecide
import ConfigurationLoader._
import LoadMethods.createGraph
import Statistics.setNumberOfNodes

// Define the main object that will be the entry point of the application.
object Main {

  // Initialize the logger for the Main object.
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val methodName = "Main"

  // Define the main method, which is the entry point.
  def main(args: Array[String]): Unit = {
    // Begin a try-catch block to catch and handle any exceptions that occur within the main method.
    try {
      // Log the beginning of the execution.
      logger.info(s"$methodName: Starting the program")

      // Use the ConfigurationLoader singleton object to create and initialize a SparkSession.
      val spark = ConfigurationLoader.createSparkSession()
      // Log that the SparkSession has been initialized.
      logger.debug(s"$methodName: SparkSession initialized")

      // Extract the SparkContext from the SparkSession, which is needed for creating RDDs and broadcast variables, among others.
      val sc = spark.sparkContext
      // Log that the SparkContext has been extracted.
      logger.debug(s"$methodName: SparkContext extracted from SparkSession")

      // Use a custom method to create GraphX RDDs from the given node and edge files for both original and perturbed graphs.
      val originalGraphRDD = createGraph(sc, getOriginalNodesPath, getOriginalEdgesPath)
      val perturbedGraphRDD = createGraph(sc, getPerturbedNodesPath, getPerturbedEdgesPath)

      // Set the global number of nodes in the graph, which may be used for statistical calculations or graph algorithms.
      setNumberOfNodes(originalGraphRDD.numVertices)

      // Log that both graphs have been successfully loaded into RDDs.
      logger.debug(s"$methodName: Graphs loaded successfully")

      // Evaluate the random walk attack simulation using a method from the `AttackSimulation` object.
      // This will likely involve running a series of random walks on both graphs to compare their structure and identify any changes or anomalies.
      evaluateRandomWalkAndDecide(sc, originalGraphRDD, perturbedGraphRDD, getNumRandomWalks, getNumRandomWalkSteps, getSimilarityThreshold)

      // Generate a summary of the attack simulation statistics.
      val summary = Statistics.generateSummary()
      // Log the generated summary for inspection.
      logger.info(s"$methodName: Summary of attack statistics:\n$summary")

      // Retrieve the output directory and file name where the summary should be saved.
      // This would use the current time to create a unique file name for each run.
      val outputDirectory = getOutputDirectory
      val fileName = generateFileName() + ".yaml"

      // Check if the application is running on the cloud and save the summary accordingly
      if (ConfigurationLoader.isRunningOnCloud) {
        logger.info(s"$methodName: Saving summary to AWS S3")
        Statistics.saveSummaryToFile_AWS(outputDirectory, fileName)
      } else {
        logger.info(s"$methodName: Saving summary to local filesystem")
        Statistics.saveSummaryToFile(outputDirectory, fileName)
      }

      // Stop the SparkSession to release the resources.
      spark.stop()
      // Log that the program has finished successfully.
      logger.info(s"$methodName: Program finished successfully")
    } catch {
      // Catch any exceptions that might have been thrown during the execution of the main method.
      case e: Exception =>
        // Log the exception at the error level.
        logger.error(s"$methodName: An exception occurred:", e)
        // Rethrow the exception if further handling is needed or to terminate the program with an error status.
        throw e
    } finally {
      // The finally block is executed after the try-catch block regardless of whether an exception was thrown.
      // It's often used for cleanup activities.
      logger.debug(s"$methodName: Exiting the program")
    }
  }
}
