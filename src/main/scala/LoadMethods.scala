
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

object LoadMethods {

  // Define a case class to represent the Node properties based on your JSON structure
  case class NodeProperties(
                             id: Int,
                             valuableData: Boolean
                             // Include other properties if needed
                           )

  // Use json4s to parse JSON
  implicit val formats: DefaultFormats.type = DefaultFormats

  /**
   * Loads nodes from a JSON file.
   *
   * @param sc The SparkContext.
   * @param path The path to the JSON file containing node data.
   * @return RDD containing vertices for GraphX.
   */
  def loadNodes(sc: SparkContext, path: String): RDD[(VertexId, Boolean)] = {
    // Read the file as an RDD[String]
    val fileContent = sc.textFile(path)

    // Parse the JSON lines to `NodeProperties`
    val nodes = fileContent.map { line =>
      val json = parse(line)
      json.extract[NodeProperties]
    }

    // Convert to RDD[(VertexId, Boolean)]
    nodes.map(node => (node.id.toLong, node.valuableData))
  }

  /**
   * Loads edges from a text file.
   *
   * @param sc The SparkContext.
   * @param path The path to the text file containing edge data.
   * @return RDD containing edges for GraphX.
   */
  def loadEdges(sc: SparkContext, path: String): RDD[Edge[Int]] = {
    // Read the file as an RDD[String]
    val fileContent = sc.textFile(path)

    // Parse the lines to create Edge objects
    val edges = fileContent.map { line =>
      val parts = line.split(" ")
      Edge(parts(0).toLong, parts(1).toLong, 1) // The edge property is set to 1 by default
    }

    edges
  }

  /**
   * Create a GraphX graph from node and edge files.
   *
   * @param sc The SparkContext.
   * @param nodePath The path to the JSON file containing node data.
   * @param edgePath The path to the text file containing edge data.
   * @return A GraphX graph.
   */
  def createGraph(sc: SparkContext, nodePath: String, edgePath: String): Graph[Boolean, Int] = {
    val vertices = loadNodes(sc, nodePath)
    val edges = loadEdges(sc, edgePath)
    Graph(vertices, edges)
  }

}
