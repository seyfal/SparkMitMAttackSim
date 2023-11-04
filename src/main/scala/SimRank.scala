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

import ConfigurationLoader.{getC, getMaxDepth, getThreshold}
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

/**
 * The SimRank object provides an implementation of the SimRank similarity algorithm on graphs.
 * It is used to compute the similarity between vertices in two graphs based on their structure.
 */
object SimRank {

  // Configuration parameters fetched from ConfigurationLoader
  private val C = getC
  private val MAX_DEPTH = getMaxDepth
  private val THRESHOLD = getThreshold

  // Logger for the SimRank object
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Recursively computes the similarity score between two vertices.
   *
   * @param neighborsMap Broadcast variable containing a map of each vertex to its neighbors.
   * @param a VertexId of the first vertex.
   * @param b VertexId of the second vertex.
   * @param depth Current depth of the recursion.
   * @return The similarity score between the two vertices.
   */
  private def compute(neighborsMap: Broadcast[Map[VertexId, Array[VertexId]]], a: VertexId, b: VertexId, depth: Int = 0): Double = {
    // Base cases for recursion
    if (depth > MAX_DEPTH) return 0.0
    if (a == b) return 1.0
    if (depth == 0) return 0.0

    // Get neighbors of both vertices
    val neighborsA = neighborsMap.value.getOrElse(a, Array.empty)
    val neighborsB = neighborsMap.value.getOrElse(b, Array.empty)

    // If either vertex has no neighbors, return 0.0 as they cannot be similar
    if (neighborsA.isEmpty || neighborsB.isEmpty) return 0.0

    // Compute similarity score for neighbors using functional constructs
    val sum = neighborsA.flatMap(i => neighborsB.map(j => compute(neighborsMap, i, j, depth + 1))).sum

    // Return the calculated score, normalized by the count of neighbors
    (C * sum) / (neighborsA.length * neighborsB.length)
  }

  /**
   * Calculates similarity scores for a list of vertices based on a random walk on the original and perturbed graphs.
   *
   * @param sc SparkContext for parallel operations.
   * @param originalGraph The original graph.
   * @param perturbedGraph The perturbed graph.
   * @param randomWalkVerticesList List of VertexId from random walks.
   * @return A list of pairs of vertex IDs and their similarity score.
   */
  def calculateForRandomWalk(
                              sc: SparkContext,
                              originalGraph: Graph[Boolean, Int],
                              perturbedGraph: Graph[Boolean, Int],
                              randomWalkVerticesList: List[VertexId]
                            ): List[((VertexId, VertexId), Double)] = {

    // Log the start of similarity computation
    logger.info("Starting calculation of SimRank for random walk vertices.")

    // Collect neighbor IDs and broadcast to all workers
    val neighborsMap = perturbedGraph.collectNeighborIds(EdgeDirection.In).collectAsMap().toMap
    val broadcastedNeighbors = sc.broadcast(neighborsMap)

    // Parallelize the random walk vertices list for distributed computation
    val randomWalkRDD = sc.parallelize(randomWalkVerticesList)

    // Calculate similarity score for each vertex in the random walk list against all vertices in the original graph
    val similarityScores = randomWalkRDD.cartesian(originalGraph.vertices)
      .filter { case (rwVertex, originalVertex) => rwVertex == originalVertex._1 }
      .flatMap { case (rwVertex, originalVertex) =>
        val simScore = compute(broadcastedNeighbors, rwVertex, originalVertex._1)
        if (simScore > THRESHOLD) Some(((rwVertex, originalVertex._1), simScore)) else None
      }
      .collect()
      .toList

    // Log the end of computation
    logger.info("Completed calculation of SimRank for random walk vertices.")

    similarityScores
  }
}
