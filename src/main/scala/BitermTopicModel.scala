import java.io.{File, PrintWriter}

import scala.collection.{mutable, Iterator}
import scala.collection.mutable.{Map, MutableList}
import scala.io.Source
import scala.util.Random

/**
 * Created by frascuchon on 1/9/15.
 */
class BitermTopicModel(val alpha: Double, val beta: Double, val numberOfTopics: Int, val iterations: Int) {

  private def generateBiterms(tokensIds: List[Int]): Iterator[(Int, Int)] = {
    tokensIds.combinations(2).map {
      case a :: b :: Nil  => (a, b)
    }
  }

  private def extractBiterms(filePath: String): (TokensList, BiTermsList) = {
    val fileSource = Source.fromFile(filePath)
    val count = Iterator.from(0)

    val tokens = fileSource.getLines().map {
      _.stripLineEnd.trim
        .split("\\s+")
        .toList match {
        case "" :: Nil => ("", List[String]())
        case fileName :: tokens => {
          (fileName, tokens)
        }
      }
    }.filter(!_._1.isEmpty)
      .toList

    fileSource.close

    val dictionary = Map[String, Int]()

    val wholeTokens = tokens.map {
      case (fileName, tokens) =>
        (fileName, tokens.map { token =>
          (token, dictionary.getOrElseUpdate(token, count.next))
        })
    }

    val biTerms = MutableList[BiTerm]()

    val documentsWithBiTerms = wholeTokens.map {
      case (fileName, tokens) => {
        val documentBiTerms = generateBiterms(tokens.unzip._2)
        val currentBiTerms = MutableList[BiTerm]()

        documentBiTerms.foreach{ bTerm =>
          currentBiTerms += bTerm
          biTerms += bTerm
        }

        (fileName, tokens, currentBiTerms)
      }
    }

    (documentsWithBiTerms, biTerms.toList)
  }

  private type TokensList = List[(String, List[(String, Int)], MutableList[BiTerm])]
  private type BiTermsList = List[BiTerm]
  private type BiTerm = (Int, Int)


  private def loadDataFromFile(filePath: String): (TokensList, Set[(String, Int)], BiTermsList) = {


    val bitermsWithTokens = extractBiterms(filePath)

    val words = bitermsWithTokens._1.flatMap {
      case (_, tokens, _) => tokens
    }.toSet

    println("tokens read: " + words.size)
    println("biterms generated: " + bitermsWithTokens._2.length)

    val (tokens, bTerms) = bitermsWithTokens
    (tokens, words, bTerms)
  }


  private def estimate(btmInfo: (TokensList, Set[(String, Int)], BiTermsList)) = {

    val (tuples, words, biterms) = btmInfo

    val m = words.size
    val bTermsZ = new Array[Int](biterms.length)
    val topicsZ = new Array[Int](numberOfTopics)
    val topics_wordsZ = new Array[Int](numberOfTopics * biterms.length)
    val table = new Array[Double](numberOfTopics)
    val theta = new Array[Double](numberOfTopics)

    val phi = new Array[Double](numberOfTopics * m)


    biterms.iterator.zipWithIndex.foreach {
      case (bTerm, index) => setTopic(bTerm, index, Random.nextInt(numberOfTopics))
    }

    for (n <- 0 until iterations) {
      println(s"iteration ${n + 1}")
      biterms.iterator.zipWithIndex.foreach { case (b, i) =>
        unsetTopic(b, bTermsZ(i))
        setTopic(b, i, sampleTopic(b))
      }
    }

    calcTheta
    calcPhi
    report

    def setTopic(b: BiTerm, i: Int, z: Int): Unit = {
      val (w1, w2) = b
      bTermsZ(i) = z
      topicsZ(z) += 1
      topics_wordsZ(w1 * numberOfTopics + z) += 1
      topics_wordsZ(w2 * numberOfTopics + z) += 1
    }

    def unsetTopic(b: BiTerm, z: Int) {
      val (w1, w2) = b
      topicsZ(z) -= 1
      topics_wordsZ(w1 * numberOfTopics + z) -= 1
      topics_wordsZ(w2 * numberOfTopics + z) -= 1
    }

    def sampleTopic(b: BiTerm): Int = {
      val (w1, w2) = b
      (0 until numberOfTopics).map { z =>
        val h = m / (topicsZ(z) * 2 + m * beta)
        val p_z_w1 = (topics_wordsZ(w1 * numberOfTopics + z) + beta) * h
        val p_z_w2 = (topics_wordsZ(w2 * numberOfTopics + z) + beta) * h

        (topicsZ(z) + alpha) * p_z_w1 * p_z_w2
      }.scanLeft(0.0)(_ + _).drop(1).copyToArray(table)
      val r = Random.nextDouble * table.last
      table.indexWhere(_ >= r)
    }

    def calcTheta = {
      (for (z <- 0 until numberOfTopics) yield
        (topicsZ(z) + alpha) / (biterms.length + numberOfTopics * alpha)
        ).copyToArray(theta)
    }

    def calcPhi = {

      (for (w <- 0 until m; z<- 0 until numberOfTopics) yield
        (topics_wordsZ(w * numberOfTopics + z) + beta) / (topicsZ(z) * 2 + m * beta)
      ).copyToArray(phi)
    }

    def report: Unit ={

      val tokens = words.map {case (a,b) => (b,a)}.toMap

      println("Topics: ")
      val o1 = new PrintWriter(new File(s"topics.k$numberOfTopics"))

      for(z <- 0 until numberOfTopics) {
        val ws = (0 until m).sortBy(w => -phi(w * numberOfTopics + z)).take(20)
        o1.println(s"$theta(z)\t" ++ ws.map(tokens).mkString("\t"))
        println(s"$theta(z)\t" ++ ws.map(tokens).mkString("\t"))
      }
      o1.close

      println("Words: ")
      val o2 = new PrintWriter(new File(s"words.k$numberOfTopics"))
      for (w <- 0 until numberOfTopics) {

        val p_w_z = (w*numberOfTopics until (w+1)*numberOfTopics-1).map(phi)
        val weight = p_w_z.zip(theta).map { case (p, q) => p * q }.toArray
        val h = 1.0 / weight.sum
        val p_z_w = weight.iterator.map(_ * h)
        o2.println(s"${tokens(w)}\t" ++ p_z_w.mkString("\t"))
        println(s"${tokens(w)}\t" ++ p_z_w.mkString("\t"))

      }


      println("Documents: ")
      val o3 = new PrintWriter(new File(s"documents.k$numberOfTopics"))

      tuples.foreach {
        case (fileName, tokens, bTerms) => {
          val bTermsSize = bTerms.size
          val hs = bTerms.map { b =>
            val (w1, w2) = b

            1.0 / (0 until numberOfTopics).map { z =>
              theta(z) * phi(w1*numberOfTopics+z) * phi(w2*numberOfTopics+z)
            }.sum / bTermsSize
          }
          println("Calculated hs...")

          val p_z_d = Iterator.range(0, numberOfTopics).map { z =>
            biterms.zip(hs).map { case (b, h) =>
              val (w1, w2) = b
              theta(z) * phi(w1*numberOfTopics+z) * phi(w2*numberOfTopics+z) * h
            }.sum
          }
          o3.println(s"${fileName}\t" ++ p_z_d.mkString("\t"))
          println(s"${fileName}\t")
          println(p_z_d.mkString("\t"))
        }
      }
      o3.close
    }

  }




  def process(filePath: String) = {
    val btmInfo = loadDataFromFile(filePath)
    estimate(btmInfo)
  }

}


object App {
  def main(args: Array[String]) {
    new BitermTopicModel(1.0 / 5, 0.01, 5, 10).process("src/test/resources/example-documents.txt")
  }
}
