import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class TestCases extends AnyFunSuite with BeforeAndAfter {

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setAppName("LoadMethodsTest")
      .setMaster("local[*]")
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  test("loadNodes should load all nodes from a JSON file") {
    val vertices = LoadMethods.loadNodes(sc, "/Users/seyfal/Desktop/CS441 Cloud/originalNodes")
    assert(vertices.count() === 501)
  }

  test("loadNodes should extract the correct 'valuableData' property") {
    val vertices = LoadMethods.loadNodes(sc, "/Users/seyfal/Desktop/CS441 Cloud/perturbedNodes")
    assert(vertices.first()._2 === false) // Replace true with the actual expected value for 'valuableData'
  }

  test("loadEdges should load all edges from a text file") {
    val edges = LoadMethods.loadEdges(sc, "/Users/seyfal/Desktop/CS441 Cloud/originalEdges")
    assert(edges.count() === 989) // Replace 10 with the actual expected number of edges
  }

  test("loadEdges should load all edges from a perturbed graph text file") {
    val edges = LoadMethods.loadEdges(sc, "/Users/seyfal/Desktop/CS441 Cloud/perturbedEdges")
    assert(edges.count() === 936) // Replace 10 with the actual expected number of edges
  }

  test("createGraph should construct a Graph from node and edge files") {
    val graph = LoadMethods.createGraph(sc, "/Users/seyfal/Desktop/CS441 Cloud/originalNodes", "/Users/seyfal/Desktop/CS441 Cloud/originalEdges")
    assert(graph.vertices.count() === 501) // Replace 5 with the actual expected number of vertices
    assert(graph.edges.count() === 989) // Replace 10 with the actual expected number of edges
  }

}
