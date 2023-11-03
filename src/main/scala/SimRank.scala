package com.lsc

import com.lsc.ConfigurationLoader.{getC, getMaxDepth, getThreshold}
import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object SimRank {

  private val C = getC
  private val MAX_DEPTH = getMaxDepth
  private val THRESHOLD = getThreshold

  private def compute(neighborsMap: Broadcast[Map[VertexId, Array[VertexId]]], a: VertexId, b: VertexId, depth: Int = 0): Double = {
    if (depth > MAX_DEPTH) return 0.0
    if (a == b) return 1.0
    if (depth == 0) return 0.0

    val neighborsA = neighborsMap.value.getOrElse(a, Array.empty)
    val neighborsB = neighborsMap.value.getOrElse(b, Array.empty)

    if (neighborsA.isEmpty || neighborsB.isEmpty) return 0.0

    val sum = (for {
      i <- neighborsA
      j <- neighborsB
    } yield compute(neighborsMap, i, j, depth + 1)).sum

    (C * sum) / (neighborsA.length * neighborsB.length)
  }

  def calculateForRandomWalk(sc: SparkContext, originalGraph: Graph[Boolean, Int], perturbedGraph: Graph[Boolean, Int], randomWalkVerticesList: List[VertexId]): List[((VertexId, VertexId), Double)] = {

    // Precompute and broadcast neighbors
    val neighborsMap = perturbedGraph.collectNeighborIds(EdgeDirection.In).collectAsMap().toMap
    val broadcastedNeighbors = sc.broadcast(neighborsMap)

    val randomWalkRDD = sc.parallelize(randomWalkVerticesList)

    randomWalkRDD.cartesian(originalGraph.vertices).filter(pair => pair._1 == pair._2._1).flatMap { case (v, origV) =>
      val simScore = compute(broadcastedNeighbors, v, origV._1)
      if (simScore > THRESHOLD) {
        Some((v, origV._1), simScore)
      } else {
        None
      }
    }.collect().toList
  }
}