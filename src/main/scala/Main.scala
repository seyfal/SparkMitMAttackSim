/*
    Copyright (c) 2023. Seyfal Sultanov
    Unless required by applicable law or agreed to in writing, software distributed under
    the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied.  See the License for the specific language governing permissions and limitations under the License.
*/

package com.lsc

// for anything that needs NetGameSim classes
import NetGraphAlgebraDefs.NodeObject

// for logging
import org.slf4j.LoggerFactory

// for locally implemented methods
import com.lsc.LoadMethods.{deserializeWithCustomClassLoader, saveToGraphX}
import com.lsc.AttackSimulation.evaluateRandomWalkAndDecide

// for Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.Graph

// for utilities
import java.io.FileInputStream
import scala.util.Random


object Main {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val methodName = "Main"

  def main(args: Array[String]): Unit = {
    try {

      // TODO: Fix the logger issue
      logger.info(s"$methodName: Starting the program")

      // Initialize SparkSession
      val spark = SparkSession.builder
        .appName("SparkRandomWalk")
        .master("local[*]") // Use "local[*]" for local mode. Change this if deploying on a cluster.
        .getOrCreate()

      // Get SparkContext from SparkSession
      val sc = spark.sparkContext

      // TODO - Change the path to call a config loader method for retrieval
      val deserializedList: List[NodeObject] = deserializeWithCustomClassLoader[List[NodeObject]](new FileInputStream("/Users/seyfal/Desktop/CS441 Cloud/NetGraph_25-10-23-13-45-45.ngs"))

      // Convert the deserialized list into a GraphX graph
      var graph = saveToGraphX(deserializedList, sc)

      // Randomly select a vertex ID to hold valuable data
      val verticesWithValuableData = graph.vertices.map(_._1).collect()
      val randomVertexId = verticesWithValuableData(Random.nextInt(verticesWithValuableData.length))

      // Create a new vertices RDD with one node assigned valuable data
      val newVertices = graph.vertices.map { case (vid, _) =>
        if (vid == randomVertexId) {
          (vid, true) // This node has valuable data
        } else {
          (vid, false) // Other nodes do not
        }
      }

      // Create a new graph with the updated vertices
      graph = graph.outerJoinVertices(newVertices) {
        case (_, _, Some(hasValuableData)) => hasValuableData
        case (_, data, None) => data
      }

      // print the graph
      println(graph.vertices.collect().mkString("\n") )

      val originalGraph: Graph[Boolean, Int] = graph
      val perturbedGraph: Graph[Boolean, Int] = graph

      evaluateRandomWalkAndDecide(sc, originalGraph, perturbedGraph, 100, 10, 0.8)

      // stop SparkSession
      spark.stop()
    } catch {
      case e: Exception =>
        logger.error(s"$methodName: An exception occurred: $e")
    }
  }

}