/*
    Copyright (c) 2023. Seyfal Sultanov
    Unless required by applicable law or agreed to in writing, software distributed under
    the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied.  See the License for the specific language governing permissions and limitations under the License.
*/

package com.lsc

import NetGraphAlgebraDefs.NodeObject
import com.lsc.LoadMethods.{deserializeWithCustomClassLoader, saveToGraphX}
import org.apache.spark.sql.SparkSession

import java.io.FileInputStream

object Main {

  def main(args: Array[String]): Unit = {

    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("SparkRandomWalk")
      .master("local[*]") // Use "local[*]" for local mode. Change this if deploying on a cluster.
      .getOrCreate()

    // Get SparkContext from SparkSession
    val sc = spark.sparkContext

    // Example usage
    val deserializedList: List[NodeObject] = deserializeWithCustomClassLoader[List[NodeObject]](new FileInputStream("/Users/seyfal/Desktop/CS441 Cloud/NetGraph_25-10-23-13-45-45.ngs"))

    // Convert the deserialized list into a GraphX graph
    val graph = saveToGraphX(deserializedList, sc)

    // stop SparkSession
    spark.stop()
  }
}
