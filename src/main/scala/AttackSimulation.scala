package com.lsc

import org.apache.spark.graphx._
import org.apache.spark.SparkContext

object AttackSimulation {

  def evaluateRandomWalkAndDecide(
                                   sc: SparkContext,
                                   originalGraph: Graph[Boolean, Int], // Boolean indicates if node has valuable data
                                   perturbedGraph: Graph[Boolean, Int],
                                   numRandomWalks: Int, // Number of random walks to perform
                                   numSteps: Int, // Number of steps in each random walk
                                   threshold: Double // Threshold for deciding a match based on SimRank
                                 ): Unit = {

    for (_ <- 1 to numRandomWalks) {
      // Generate a random walk path
      val randomWalkPath = RandomWalks.RandomWalkMethods.randomWalk(perturbedGraph, numSteps)
      // Add the random walk length to the statistics
      Statistics.addRandomWalkLength(randomWalkPath.length)

      // Evaluate the random walk path
      val simRankScores = SimRank.calculateForRandomWalk(sc, originalGraph, perturbedGraph, randomWalkPath)

      // Determine the success of the attack
      simRankScores.foreach { case ((_, originalId), score) =>
        val originalNodeContainsValuableData = originalGraph.vertices.filter {
          case (vid, valuableData) => vid == originalId && valuableData
        }.count() > 0

        if (score > threshold) {
          if (originalNodeContainsValuableData) {
            // True positive: attack correctly identified valuable data
            Statistics.addTruePositive()
          } else {
            // False positive: attack incorrectly identified node as having valuable data
            Statistics.addFalsePositive()
          }
        } else {
          if (originalNodeContainsValuableData) {
            // False negative: attack failed to identify a node that did have valuable data
            Statistics.addFalseNegative()
          } else {
            // True negative: correctly identified a node as not having valuable data
            Statistics.addTrueNegative()
          }
        }
      }
    }
  }
}
