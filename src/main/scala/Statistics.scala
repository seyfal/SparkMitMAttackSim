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

import scala.collection.mutable
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.ObjectMetadata

/** Object holding various statistics functions and saving results to files or AWS S3.
 *
 *  This object avoids mutable state and relies on functional programming principles.
 */
object Statistics {

  // These are mutable placeholders for statistics gathered during execution
  val randomWalkLengths: mutable.ListBuffer[Int] = mutable.ListBuffer()
  var truePositives = 0
  var falsePositives = 0
  var trueNegatives = 0
  var falseNegatives = 0
  var numberOfNodes = 0

  // Call this method when a random walk is completed
  def addRandomWalkLength(length: Int): Unit = {
    randomWalkLengths += length
  }

  // Call these methods when an attack is evaluated
  def addTruePositive(): Unit = truePositives += 1
  def addFalsePositive(): Unit = falsePositives += 1
  def addTrueNegative(): Unit = trueNegatives += 1
  def addFalseNegative(): Unit = falseNegatives += 1

  // Calculate precision and recall
  def precision: Double = truePositives.toDouble / (truePositives + falsePositives)
  def recall: Double = truePositives.toDouble / (truePositives + falseNegatives)

  // Additional metrics
  def accuracy: Double = (truePositives + trueNegatives).toDouble / (truePositives + trueNegatives + falsePositives + falseNegatives)
  def fMeasure: Double = 2 * (precision * recall) / (precision + recall)
  def successfulAttackRatio: Double = truePositives.toDouble / randomWalkLengths.size

  // Statistics for random walks
  def minWalkLength: Int = randomWalkLengths.min
  def maxWalkLength: Int = randomWalkLengths.max
  def meanWalkLength: Double = randomWalkLengths.sum.toDouble / randomWalkLengths.size
  def medianWalkLength: Double = {
    val sortedLengths = randomWalkLengths.sorted
    val size = sortedLengths.size
    if (size % 2 == 0) {
      (sortedLengths(size / 2 - 1) + sortedLengths(size / 2)).toDouble / 2
    } else {
      sortedLengths(size / 2).toDouble
    }
  }

  def setNumberOfNodes(nodes: Long): Unit = {
    numberOfNodes = nodes.toInt
  }

  // Generate a summary of the statistics
  def generateSummary(): String = {
    s"""
       |number_of_random_walks: ${randomWalkLengths.size}
       |number_of_nodes: $numberOfNodes
       |successful_attacks: $truePositives
       |failed_attacks: ${falsePositives + falseNegatives}
       |precision: $precision
       |recall: $recall
       |accuracy: $accuracy
       |f_measure: $fMeasure
       |min_walk_length: $minWalkLength
       |max_walk_length: $maxWalkLength
       |mean_walk_length: $meanWalkLength
       |median_walk_length: $medianWalkLength
       |ratio_of_successful_attacks: $successfulAttackRatio
       |""".stripMargin.trim
  }

  def saveSummaryToFile(outputDirectory: String, fileName: String): Unit = {
    val yamlContent = generateSummary()
    val filePath = Paths.get(outputDirectory, fileName)

    // Ensure the directory exists
    Files.createDirectories(filePath.getParent)

    // Write the YAML content to the file, create new file or overwrite if it already exists
    Files.write(filePath, yamlContent.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def saveSummaryToFile_AWS(outputDirectory: String, fileName: String): Unit = {
    val yamlContent = generateSummary()
    val s3Client = AmazonS3ClientBuilder.standard().build()

    // Assuming outputDirectory is of the form "s3://bucketName/path/to/folder"
    val bucketName = outputDirectory.split("/")(2)
    val key = outputDirectory.substring(outputDirectory.indexOf(bucketName) + bucketName.length + 1) + "/" + fileName

    // Create metadata for your object and set content length
    val metadata = new ObjectMetadata()
    metadata.setContentLength(yamlContent.length)

    // Upload a file as a new object with ContentType and title specified.
    val inputStream = new java.io.ByteArrayInputStream(yamlContent.getBytes("UTF-8"))
    val putRequest = new PutObjectRequest(bucketName, key, inputStream, metadata)

    s3Client.putObject(putRequest)
  }

}
