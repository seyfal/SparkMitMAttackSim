/*
    Copyright (c) 2023. Seyfal Sultanov
    Unless required by applicable law or agreed to in writing, software distributed under
    the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied.  See the License for the specific language governing permissions and limitations under the License.
*/

package com.lsc

// Required imports for GraphX
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object RandomWalks {
  // Initialize logger for this object
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)
  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD
  import org.slf4j.LoggerFactory
  import scala.util.Random

  object RandomWalkMethods {

    // Initialize logger for this object
    private val logger = LoggerFactory.getLogger(getClass)

    /**
     *  Conducts a random walk on a given graph.
     *
     *  @param graph The input graph with Boolean vertex values and Int edge values.
     *  @param maxSteps The maximum number of steps for the random walk. Default value is 5.
     *  @return A list of vertex IDs representing the path taken in the random walk.
     */
    def randomWalk(graph: Graph[Boolean, Int], numSteps: Int): List[Long] = {
      // Get all vertex IDs from the graph
      val vertexIds = graph.vertices.map(_._1).collect()

      // Randomly select a starting vertex
      val startVertexId = vertexIds(Random.nextInt(vertexIds.length))

      // Initialize the path with the starting vertex
      var path = List(startVertexId)
      var currentVertexId = startVertexId

      // Perform the random walk
      for (_ <- 1 until numSteps) {
        val neighbors = graph.collectNeighborIds(EdgeDirection.Out).lookup(currentVertexId).head

        if (neighbors.nonEmpty) {
          currentVertexId = neighbors(Random.nextInt(neighbors.length))
          path = path :+ currentVertexId
        } else {
          // No neighbors, end the walk
          return path
        }
      }

      path
    }
  }

}

