package com.frascuchon.ml.btm

import org.apache.flink.api.scala._
import scopt.OptionParser

import scala.io.Source
import scala.util.Random


/**
 * Created by frascuchon on 3/9/15.
 */
object BitermTopicModelMain {

  private val parser = new OptionParser[Config]("btm") {
    head("btm", "1.0")
    opt[String]('i', "input") action {
      (x, c) => c.copy(inputPath = x)
    } text ("input file path")
    opt[Int]('k', "topic") action {
      (v, c) => c.copy(topics = v)
    } text ("number of topics")
    opt[Double]('a', "alpha") action {
      (v, c) => c.copy(alpha = v)
    } text ("alpha factor")
    opt[Double]('b', "beta") action {
      (v, c) => c.copy(beta = v)
    } text ("beta factor")
    opt[Int]('n', "iter") action {
      (v, c) => c.copy(iterations = v)
    } text ("algorithm iterations")

    checkConfig {
      c => {
        if (c.inputPath.isEmpty) failure("must specify input path")
        if (c.iterations < 0) failure("mut specify positive value for iterations")
        if (c.topics < 0) failure("mut specify positive value for topics")
        else success
      }
    }
  }

  def main(args: Array[String]) {

    parser.parse(args, Config()) match {
      case Some(config) =>

        val env = ExecutionEnvironment.getExecutionEnvironment

        val documents = readDocuments(config.inputPath)
        val wordsWithIds = extractWords(documents).zipWithIndex

        val bitermsByDocument = extractBiterms(documents.map { case (_, words) => words.map(wordsWithIds.toMap) })
        val biterms = bitermsByDocument.flatMap(a => a)

        val dictionarySize = wordsWithIds.length

        val topicWordAssignation: Array[Array[Int]] = Array.tabulate(config.topics, dictionarySize)((_, _) => 0)
        val topicsAssignation: Array[Int] = new Array[Int](config.topics)

        def setTopic(topic: Int, w1: Int, w2: Int) = {
          topicsAssignation(topic) += 1
          topicWordAssignation(topic)(w1) += 1
          topicWordAssignation(topic)(w2) += 1
        }

        def unsetTopic(topic: Int, w1: Int, w2: Int) = {
          topicsAssignation(topic) -= 1
          topicWordAssignation(topic)(w1) -= 1
          topicWordAssignation(topic)(w2) -= 1
        }

        def sampleTopic(w1: Int, w2: Int) = {
          val table = (0 until config.topics).map { z =>
            val h = dictionarySize / (topicsAssignation(z) * 2 + dictionarySize * config.beta)
            val p_z_w1 = (topicWordAssignation(z)(w1) + config.beta) * h
            val p_z_w2 = (topicWordAssignation(z)(w2) + config.beta) * h

            (topicsAssignation(z) + config.alpha) * p_z_w1 * p_z_w2
          }.scanLeft(0.0)(_ + _).drop(1)

          val r = Random.nextDouble * table.last
          table.indexWhere(_ >= r)
        }

        val bitermsDataset = env.fromCollection(biterms.map { case (w1, w2) => {
          val initTopic = Random.nextInt(config.topics)
          setTopic(initTopic, w1, w2)
          BitermData((w1, w2), initTopic)
        }
        })

        val result = bitermsDataset.iterate(config.iterations) {
          currentBitermsDataSet =>
            currentBitermsDataSet.map { bitermData =>
              val (w1, w2) = bitermData.biterm

              unsetTopic(bitermData.topic, w1, w2)
              val newTopic = sampleTopic(w1, w2)
              setTopic(newTopic, w1, w2)

              BitermData((w1, w2), newTopic)
            }
        }

        val theta = topicsAssignation.map(topicAssign => (topicAssign + config.alpha) * 1.0 / (biterms.length + config.topics * config.alpha))
        val phi = topicWordAssignation.zipWithIndex.map {
          case (wordsAssignations, topic) => wordsAssignations.map {
            wordAssignation => (wordAssignation + config.beta) / (topicsAssignation(topic) * 2 + dictionarySize * config.beta)
          }
        }

        val finalDistribution = env.fromCollection(documents.zip(bitermsByDocument)).map {
          tuple => {
            val ((filename, _), biterms) = tuple

            val hs = biterms.map { b =>
              val (w1, w2) = b

              1.0 / (0 until config.topics).map { z => theta(z) * phi(z)(w1) * phi(z)(w2) }.sum / biterms.size
            }

            (filename, (0 until config.topics).map { z =>
              biterms.zip(hs).map { case (b, h) =>
                val (w1, w2) = b

                theta(z) * phi(z)(w1) * phi(z)(w2) * h
              }.sum
            })
          }
        }

        finalDistribution.print()


      case None => sys.exit(1)
    }

  }

  private def extractWords(documents: Array[(String, Seq[String])]): Array[String] = {
    documents.flatMap {
      case (filename, words) => words
    }.distinct
  }

  private def extractBiterms(documents: Array[Seq[Int]]): Array[Array[(Int, Int)]] = {

    def generateBiterms(tokensIds: Seq[Int]): Array[(Int, Int)] = {
      tokensIds.combinations(2).map {
        case Vector(a, b) => (a, b)
      }.toArray
    }

    documents map generateBiterms
  }

  private def readDocuments(filePath: String): Array[(String, Seq[String])] = {
    val fileSource = Source.fromFile(filePath)
    val tokens = fileSource.getLines().filter(_.nonEmpty).map {
      _.stripLineEnd.trim
        .split("\\s+") match {
        case Array(fileName, tokens@_*) => (fileName, tokens)
      }
    }
    tokens.toArray
  }

  case class Config(inputPath: String = "",
                    iterations: Int = 20,
                    topics: Int = 5,
                    alpha: Double = 0.2,
                    beta: Double = 0.01)

  case class BitermData(biterm: (Int, Int), topic: Int)

}


