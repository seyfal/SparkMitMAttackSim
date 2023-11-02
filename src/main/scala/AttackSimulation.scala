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

    // This will hold the results of all attacks
    var successfulAttacks = 0
    var failedAttacks = 0

    for (_ <- 1 to numRandomWalks) {
      // Generate a random walk path
      val randomWalkPath = RandomWalks.RandomWalkMethods.randomWalk(perturbedGraph, numSteps)

      // Evaluate the random walk path
      val simRankScores = SimRank.calculateForRandomWalk(sc, originalGraph, perturbedGraph, randomWalkPath)

      // Determine the success of the attack
      simRankScores.foreach { case ((perturbedId, originalId), score) =>
        if (score > threshold) {
          // Check if the original node contains valuable data
          val originalNodeContainsValuableData = originalGraph.vertices.filter {
            case (vid, valuableData) => vid == originalId && valuableData
          }.count() > 0

          if (originalNodeContainsValuableData) {
            // Check if the match is correct (not a honeypot)
            if (perturbedGraph.vertices.filter {
              case (vid, _) => vid == perturbedId
            }.count() == 1) {
              successfulAttacks += 1
            } else {
              failedAttacks += 1
            }
          }
        }
      }
    }

    // Print the statistics for the simulation
    println(s"Number of successful attacks: $successfulAttacks")
    println(s"Number of failed attacks: $failedAttacks")

    // You can add additional logic for precision, recall, or any other metrics here.
  }

}


