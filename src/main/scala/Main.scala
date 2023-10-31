/*
    Copyright (c) 2023. Seyfal Sultanov
    Unless required by applicable law or agreed to in writing, software distributed under
    the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied.  See the License for the specific language governing permissions and limitations under the License.
*/

package com.lsc

// for anything that needs NetGameSim classes
import NetGraphAlgebraDefs.NodeObject

// for randomWalk
import com.lsc.RandomWalks.RandomWalkMethods.randomWalk

// for logging
import org.slf4j.LoggerFactory

// for locally implemented methods
import com.lsc.LoadMethods.{deserializeWithCustomClassLoader, saveToGraphX}

// for Spark
import org.apache.spark.sql.SparkSession

// for reading from file
import java.io.FileInputStream

object Main {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val methodName = "Main"

  def main(args: Array[String]): Unit = {
    try {

      logger.info(s"$methodName: Starting the program")

      // Initialize SparkSession
      val spark = SparkSession.builder
        .appName("SparkRandomWalk")
        .master("local[*]") // Use "local[*]" for local mode. Change this if deploying on a cluster.
        .getOrCreate()

      // Get SparkContext from SparkSession
      val sc = spark.sparkContext

      val deserializedList: List[NodeObject] = deserializeWithCustomClassLoader[List[NodeObject]](new FileInputStream("/Users/seyfal/Desktop/CS441 Cloud/NetGraph_25-10-23-13-45-45.ngs"))

      // Convert the deserialized list into a GraphX graph
      val graph = saveToGraphX(deserializedList, sc)

      // Specify the number of steps for each random walk
      val numSteps = 5

      // Run random walks 20 times
      val numRuns = 20
      val allPaths = for (_ <- 1 to numRuns) yield randomWalk(graph, numSteps)

      // Print the paths
      allPaths.zipWithIndex.foreach { case (path, index) =>
        println(s"Random Walk ${index + 1}: ${path.mkString(" -> ")}")
      }

      // stop SparkSession
      spark.stop()
    } catch {
      case e: Exception =>
        logger.error(s"$methodName: An exception occurred: $e")
    }
  }

}