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
import org.apache.spark.graphx._
import scala.util.Random

/** Singleton object for random walk simulations on graphs using GraphX. */
object RandomWalks {
  // Initialize logger for this object
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  /** Contains methods related to random walk operations on graphs. */
  object RandomWalkMethods {

    /**
     * Conducts a recursive random walk on a given graph.
     *
     * @param graph The input graph with Boolean vertex values and Int edge values.
     * @param numSteps The maximum number of steps for the random walk.
     * @return A list of vertex IDs representing the path taken in the random walk.
     */
    def randomWalk(graph: Graph[Boolean, Int], numSteps: Int): List[Long] = {
      logger.info("Starting random walk on graph.")

      // Helper function to perform recursive random walk.
      @scala.annotation.tailrec
      def walk(currentVertexId: Long, stepsLeft: Int, path: List[Long]): List[Long] = {
        logger.debug(s"Current vertex: $currentVertexId, steps left: $stepsLeft")
        if (stepsLeft == 0) {
          logger.info("Random walk completed.")
          path
        } else {
          val neighbors = graph.collectNeighborIds(EdgeDirection.Out).lookup(currentVertexId).headOption.getOrElse(Array.empty[Long])
          if (neighbors.nonEmpty) {
            val nextVertexId = neighbors(Random.nextInt(neighbors.length))
            logger.debug(s"Walking to next vertex: $nextVertexId")
            walk(nextVertexId, stepsLeft - 1, path :+ nextVertexId)
          } else {
            logger.info("No neighbors found, ending walk early.")
            path
          }
        }
      }

      val vertexIds = graph.vertices.map(_._1).collect()
      if (vertexIds.isEmpty) {
        logger.error("The graph contains no vertices to begin a random walk.")
        List.empty[Long]
      } else {
        val startVertexId = vertexIds(Random.nextInt(vertexIds.length))
        logger.info(s"Starting vertex chosen at random: $startVertexId")
        walk(startVertexId, numSteps, List(startVertexId))
      }
    }
  }
}
