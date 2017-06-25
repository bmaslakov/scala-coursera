package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` if the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var count = 0
    var i = 0
    while (count >= 0 && i < chars.length) {
      if (chars(i) == '(') {
        count += 1
      } else if (chars(i) == ')') {
        count -= 1
      }
      i += 1
    }
    count == 0
  }

  /** Returns `true` if the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
    def traverse(idx: Int, until: Int/*, arg1: Int, arg2: Int*/): (Int, Int) /*: ???*/ = {
      var open = 0
      var close = 0
      var i = idx
      while (i < until) {
        if (chars(i) == '(') {
          open += 1
        } else if (chars(i) == ')') {
          close += 1
        }
        i += 1
      }
      val tuple = (Math.max(0, open - close), Math.max(0, close - open))
//      println(s"traverse: ${chars.subSequence(idx, until)}: ($tuple)")
      tuple
    }

    def reduce(from: Int, until: Int): (Int, Int) /*: ???*/ = {
      if (until - from <= threshold) {
        traverse(from, until)
      } else {
        val mid = (from + until) / 2
        val (left, right) = parallel(reduce(from, mid), reduce(mid, until))
        val open  = Math.max(0, left._1 - right._2) + right._1
        val close = Math.max(0, right._2 - left._1) + left._2
//        println(s"reduce: ${chars.subSequence(from, until)}: ($open, $close)")
        (open, close)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!
}
