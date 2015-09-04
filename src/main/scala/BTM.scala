import java.io.{File, PrintWriter}

import org.slf4j.LoggerFactory

import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random


/**
 * Created by dvilasuero on 05/08/15.
 */

/**
 * Experimental implementation of BTM (Biterm topic modelling) [X. Yan et al WWW2013]
 */
class BTM(val alpha: Double, val beta: Double, val k:Int, val maxIterations:Int) {

  val log = LoggerFactory.getLogger(classOf[BTM]);
  private var words:Array[String] = null
  private var documents:Array[(String, Array[Int])] = null
  private var biterms:Array[(Int, Int)] = null
  private var m:Int = 0
  private var biterms_z:Array[Int] = null
  private var topics_z:Array[Long] = null
  private var topics_words_z:Array[Long] = null
  private var theta:Array[Double] = null
  private var phi:Array[Double] = null
  private var table:Array[Double] = null

  def loadFromMap(s:ListBuffer[(String, String)]): Unit = {


    val dict = mutable.HashMap[String,Int]()
    val count = Iterator.from(0)
    val buf1 = mutable.ArrayBuffer[(String, Array[Int])]()
    val buf2 = mutable.ArrayBuffer[(Int, Int)]()
    //val s = Source.fromFile(file)

    for (fileTuple <- s) {
      //data += new Tuple2(m._1, m._2.mkString(" "))

      val wordsIds = fileTuple._2.split(" ").map(word => dict.getOrElseUpdate(word, count.next))
      buf1 += ((fileTuple._1, wordsIds))
      buf2 ++= getBiterms(wordsIds)
    }

    m = count.next

    words = new Array(m)

    dict.iterator.foreach{ case(word, i) =>
      words(i) = word
    }
    documents = buf1.toArray
    biterms = buf2.toArray
    biterms_z = new Array(biterms.length)
    topics_z = new Array(k)
    topics_words_z = new Array(k * m)
    theta = new Array(k)
    phi = new Array(k * m)
    table = new Array(k)

    log.info(s"|B|: ${biterms.length}, K: $k, M: $m")

  }

  def load(file:String): Unit = {

    log.info(s"Started loading $file")
    val dict = mutable.HashMap[String,Int]()
    val count = Iterator.from(0)
    val buf1 = mutable.ArrayBuffer[(String, Array[Int])]()
    val buf2 = mutable.ArrayBuffer[(Int, Int)]()
    val s = Source.fromFile(file)

    try {
      s.getLines().foreach{ l =>
        if(!l.isEmpty) {
          val row = l.stripLineEnd.split(" ")
          val d = row.tail.map(word => dict.getOrElseUpdate(word, count.next))
          buf1 += ((row.head, d))
          buf2 ++= getBiterms(d)
        }
      }
    } finally {
      s.close()
    }

    m = count.next

    words = new Array(m)
    dict.iterator.foreach{ case(word, i) =>
      words(i) = word
    }
    documents = buf1.toArray
    biterms = buf2.toArray
    biterms_z = new Array(biterms.length)
    topics_z = new Array(k)
    topics_words_z = new Array(k * m)
    theta = new Array(k)
    phi = new Array(k * m)
    table = new Array(k)

    println(s"|B|: ${biterms.length}, K: $k, M: $m")
  }


  def estimate {
    Iterator.continually(0L).copyToArray(topics_z)
    Iterator.continually(0L).copyToArray(topics_words_z)

    biterms.iterator.zipWithIndex.foreach { case (b, i) =>
      setTopic(b, i, Random.nextInt(k))
    }

    Iterator.range(0, maxIterations).foreach { n =>
      println(s"iteration ${n+1}")
      biterms.iterator.zipWithIndex.foreach { case (b, i) =>
        unsetTopic(b, biterms_z(i))
        setTopic(b, i, sampleTopic(b))
      }
    }

    calcTheta
    calcPhi

    println("done!")
  }

  def report: Unit ={

    println("Topics: ")

    val o1 = new PrintWriter(new File(s"topics.k$k"))
    Iterator.range(0, k).foreach { z =>
      val ws = (0 until m).sortBy(w => -phi(w*k+z)).take(20)
      o1.println(s"$theta(z)\t" ++ ws.map(words).mkString("\t"))
      println(s"$theta(z)\t" ++ ws.map(words).mkString("\t"))
    }
    o1.close

    println("Words: ")
    val o2 = new PrintWriter(new File(s"words.k$k"))
    Iterator.range(0, k).foreach { w =>
      val p_w_z = Iterator.range(w*k, (w+1)*k-1).map(phi)
      val weight = p_w_z.zip(theta.iterator).map { case (p, q) => p * q }.toArray
      val h = 1.0 / weight.sum
      val p_z_w = weight.iterator.map(_ * h)
      o2.println(s"${words(w)}\t" ++ p_z_w.mkString("\t"))
      println(s"${words(w)}\t" ++ p_z_w.mkString("\t"))
    }


    println("Documents: ")
    val o3 = new PrintWriter(new File(s"documents.k$k"))
    documents.foreach { case (id, d) =>
      val bs = getBiterms(d).toArray
      val hs = bs.map { b =>

        val (w1, w2) = b

        // val count = bs.count(_ == b)
        //if (count > 1) println("fawn !")

        1.0 / Iterator.range(0, k).map { z =>
          theta(z) * phi(w1*k+z) * phi(w2*k+z)
        }.sum / bs.length
      }
      println("Calculated hs: "+ hs.toString)
      val p_z_d = Iterator.range(0, k).map { z =>
        bs.iterator.zip(hs.iterator).map { case (b, h) =>
          val (w1, w2) = b
          theta(z) * phi(w1*k+z) * phi(w2*k+z) * h
        }.sum
      }
      o3.println(s"${id}\t" ++ p_z_d.mkString("\t"))
      println(s"${id}\t")
      println(p_z_d.mkString("\t"))

    }
    o3.close
    
  }





  private def getBiterms(d:Array[Int]):Iterator[(Int, Int)] = {
    d.toSeq.combinations(2).map { case Seq(w1, w2) =>
      if (w1 < w2) (w1, w2) else (w2, w1)
    }
  }

  private def setTopic(b:(Int, Int), i:Int, z:Int) {
    val (w1, w2) = b
    biterms_z(i) = z
    topics_z(z) += 1
    topics_words_z(w1*k+z) += 1
    topics_words_z(w2*k+z) += 1
  }

  private def unsetTopic(b:(Int, Int), z:Int) {
    val (w1, w2) = b
    topics_z(z) -= 1
    topics_words_z(w1*k+z) -= 1
    topics_words_z(w2*k+z) -= 1
  }

  private def sampleTopic(b:(Int, Int)):Int = {
    val (w1, w2) = b
    Iterator.range(0, k).map { z =>
      val h = m / (topics_z(z) * 2 + m * beta)
      val p_z_w1 = (topics_words_z(w1*k+z) + beta) * h
      val p_z_w2 = (topics_words_z(w2*k+z) + beta) * h

      (topics_z(z) + alpha) * p_z_w1 * p_z_w2
    }.scanLeft(0.0)(_ + _).drop(1).copyToArray(table)
    val r = Random.nextDouble * table.last
    table.indexWhere(_ >= r)
  }

  private def calcTheta {
    Iterator.range(0, k).map { z =>
      (topics_z(z) + alpha) / (biterms.length + k * alpha)
    }.copyToArray(theta)
  }

  private def calcPhi {
    Iterator.range(0, m).flatMap { w =>
      Iterator.range(0, k).map { z =>
        (topics_words_z(w*k+z) + beta) / (topics_z(z) * 2 + m * beta)
      }
    }.copyToArray(phi)
  }
}

object BTM {
  def main(args:Array[String]) {
    val btm = new BTM(1.0 / 5, 0.01, 5, 10)
    btm.load("src/test/resources/example-documents.txt")
    btm.estimate
    btm.report
  }



}
