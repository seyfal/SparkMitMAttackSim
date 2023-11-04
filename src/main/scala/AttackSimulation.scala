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

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.slf4j.LoggerFactory

// Other imports might be required, such as:
// import org.apache.spark._
// import org.apache.spark.graphx.lib._

/**
 * Object to simulate attack on graphs using random walks and evaluate using SimRank.
 */
object AttackSimulation {

  // Logger for the AttackSimulation
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Evaluates random walks on a perturbed graph and decides the success of an attack based on SimRank scores.
   *
   * @param sc              The Spark context.
   * @param originalGraph   The original graph with nodes marked for valuable data.
   * @param perturbedGraph  The perturbed graph to be analyzed.
   * @param numRandomWalks  The number of random walks to perform.
   * @param numSteps        The number of steps in each random walk.
   * @param threshold       The threshold for deciding a match based on SimRank.
   */

  def evaluateRandomWalkAndDecide(
                                   sc: SparkContext, // Ensure SparkContext is imported and available
                                   originalGraph: Graph[Boolean, Int],
                                   perturbedGraph: Graph[Boolean, Int],
                                   numRandomWalks: Int,
                                   numSteps: Int,
                                   threshold: Double
                                 ): Unit = {
    logger.info("Starting evaluation of random walks.")

    // Assuming `calculateForRandomWalk` returns a List, we remove the Array type declaration.
    // If `calculateForRandomWalk` should return an Array, make sure that method's return type is Array.
    (1 to numRandomWalks).foreach { _ =>
      val randomWalkPath = RandomWalks.RandomWalkMethods.randomWalk(perturbedGraph, numSteps)
      Statistics.addRandomWalkLength(randomWalkPath.length)

      val simRankScores = SimRank.calculateForRandomWalk(sc, originalGraph, perturbedGraph, randomWalkPath).toArray // Convert to Array if needed
      processSimRankScores(originalGraph, simRankScores, threshold)
    }

    logger.info("Completed evaluation of random walks.")
  }

  /**
   * Processes the SimRank scores to determine the success of the attack.
   *
   * @param originalGraph The original graph with nodes marked for valuable data.
   * @param simRankScores The SimRank scores for random walks.
   * @param threshold     The threshold for deciding a match based on SimRank.
   */
  private def processSimRankScores(
                                    originalGraph: Graph[Boolean, Int],
                                    simRankScores: Array[((VertexId, VertexId), Double)],
                                    threshold: Double
                                  ): Unit = {
    simRankScores.foreach { case ((_, originalId), score) =>
      val originalNodeContainsValuableData = originalGraph.vertices
        .filter { case (vid, valuableData) => vid == originalId && valuableData }
        .count() > 0

      // Determine the success of the attack based on the SimRank score and the presence of valuable data
      evaluateAttackSuccess(score, originalNodeContainsValuableData, threshold)
    }
  }

  /**
   * Evaluates the success of the attack based on the SimRank score and the presence of valuable data.
   *
   * @param score                         The SimRank score.
   * @param originalNodeContainsValuableData Indicates if the original node contains valuable data.
   * @param threshold                     The threshold for deciding a match based on SimRank.
   */
  private def evaluateAttackSuccess(
                                     score: Double,
                                     originalNodeContainsValuableData: Boolean,
                                     threshold: Double
                                   ): Unit = {
    if (score > threshold) {
      if (originalNodeContainsValuableData) {
        Statistics.addTruePositive()
      } else {
        Statistics.addFalsePositive()
      }
    } else {
      if (originalNodeContainsValuableData) {
        Statistics.addFalseNegative()
      } else {
        Statistics.addTrueNegative()
      }
    }
  }
}
