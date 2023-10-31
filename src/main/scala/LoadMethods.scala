/*
    Copyright (c) 2023. Seyfal Sultanov
    Unless required by applicable law or agreed to in writing, software distributed under
    the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied.  See the License for the specific language governing permissions and limitations under the License.
*/

package com.lsc

// for deserializeWithCustomClassLoader
import NetGraphAlgebraDefs.NetGraphComponent

import java.io._

// for graphToGraphX
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

// for NodeObject
import NetGraphAlgebraDefs.NodeObject
import NetGraphAlgebraDefs.Action

object LoadMethods {

  /**
   * Deserializes an object from the given `InputStream` using a custom `ClassLoader`.
   *
   * This method uses the current thread's context class loader to resolve the classes
   * during the deserialization process. This can be useful in situations where the
   * standard class loader might not have access to all classes, especially in environments
   * like certain application servers or when dealing with dynamically loaded classes.
   *
   * @param inputStream The input stream from which to read the serialized object.
   * @tparam T The type of object to be deserialized.
   * @return The deserialized object.
   * @throws IOException            If an I/O error occurs while reading the stream.
   * @throws ClassNotFoundException If a class of a serialized object cannot be found.
   */
  def deserializeWithCustomClassLoader[T](inputStream: InputStream): T = {
    // Fetch the current thread's context class loader
    val classLoader = Thread.currentThread().getContextClassLoader

    // Create an `ObjectInputStream` from the provided `inputStream`.
    // This stream is specialized with a custom `resolveClass` method.
    val ois = new ObjectInputStream(inputStream) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        // For each class descriptor encountered during deserialization,
        // use the custom classLoader to fetch the corresponding class.
        Class.forName(desc.getName, false, classLoader)
      }
    }

    // Read the object from the stream and cast it to the expected type.
    ois.readObject().asInstanceOf[T]
  }

  private def extractNodeInfo(node: NodeObject): (Long, Boolean) = {
    (node.id.toLong, node.valuableData)
  }

  private def extractEdgeInfo(action: Action): Edge[Int] = {
    Edge(action.fromNode.id.toLong, action.toNode.id.toLong, 1)
  }

  def saveToGraphX(deserializedList: List[NetGraphComponent], sc: SparkContext): Graph[Boolean, Int] = {
    // Separate NodeObjects and Actions
    val nodesOnly = deserializedList.collect { case node: NodeObject => node }
    val actionsOnly = deserializedList.collect { case action: Action => action }

    // Extracting properties and converting NodeObject list to Vertex RDD
    val vertices: RDD[(VertexId, Boolean)] = sc.parallelize(nodesOnly.map(node => extractNodeInfo(node)))

    // Extract edges with cost from the Action objects
    val edgesFromActions = actionsOnly.map(action => extractEdgeInfo(action))

    val edges: RDD[Edge[Int]] = sc.parallelize(edgesFromActions)

    // Create the Graph
    val graph = Graph(vertices, edges)

    graph
  }


}
