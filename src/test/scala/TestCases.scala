//import NetGraphAlgebraDefs.NetGraphComponent
//import com.lsc.LoadMethods.saveToGraphX
//import org.scalatest._
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should.Matchers
//import org.apache.spark._
//import org.apache.spark.graphx._
//
//// for NodeObject
//import NetGraphAlgebraDefs.NodeObject
//import NetGraphAlgebraDefs.Action
//
//class TestCases extends AnyFlatSpec with Matchers {
//
//  // Mock SparkContext for testing
//  val conf = new SparkConf().setAppName("GraphTests").setMaster("local")
//  val sc = new SparkContext(conf)
//
//  "saveToGraphX" should "create correct edges from NodeObjects and Actions" in {
//    // Sample data for testing
//    val node1 = NodeObject(...)  // fill in the data
//    val node2 = NodeObject(...)  // fill in the data
//    val action1 = Action(...)    // fill in the data
//
//    val deserializedList: List[NetGraphComponent] = List(node1, node2, action1)
//
//    // Create the graph
//    val graph = saveToGraphX(deserializedList, sc)
//
//    // Verifying edges using graph's edges RDD
//    val edgeList = graph.edges.collect.toList
//
//    edgeList should contain (Edge(node1.id, node2.id, action1.actionType))
//    // ... Add more such checks for other edges
//
//    // If node1 has children, verify that as well
//    node1.childrenObjects.foreach(child => edgeList should contain (Edge(node1.id, child.id, 1)))
//    // The '1' here assumes a default edge value for parent-child relationships. Change if needed.
//  }
//
//  // You can add more tests...
//
//  // Clean up after all tests
//  def afterAll() {
//    sc.stop()
//  }
//}
