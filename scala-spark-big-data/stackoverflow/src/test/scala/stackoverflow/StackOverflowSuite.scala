package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("'scoredPostings' should return the correct output") {
    val valeus = List(
      Posting(1,6,None,None,140,None),
      Posting(1,42,Some(43),None,155,None),
      Posting(1,72,None,None,16,None),
      Posting(1,126,None,None,33,None),
      Posting(1,174,None,None,38,None),
      Posting(2,43,None,Some(42),38,None),
      Posting(2,44,None,Some(42),32,None)
    )
    val grouped = StackOverflow.sc.parallelize(valeus)
    assertResult(Array((42, List((valeus(1), valeus(5)), (valeus(1), valeus(6))))))(new StackOverflow().groupedPostings(grouped).collect())
  }
}
