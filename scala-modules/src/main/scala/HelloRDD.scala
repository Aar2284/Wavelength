package wavelength

import org.apache.spark.sql.SparkSession

case class Play(userId: String, songId: String, durationSec: Long)

object HelloRDD {

  private val plays: List[Play] = List(
    Play("u_nina", "s_blue", 214),
    Play("u_ivan", "s_green", 180),
    Play("u_nina", "s_red", 241),
    Play("u_maya", "s_green", 199)
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HelloRDD")
      .master("local[1]")
      .getOrCreate()

    val sc = spark.sparkContext

    val byUser = sc.parallelize(plays)
      .map(play => (play.userId, play.durationSec))
      .reduceByKey(_ + _)
      .collect()
      .toList
      .sortBy(-_._2)

    val genreCounts = plays.groupBy(_.songId).view.mapValues(_.size).toMap

    val headline: String = byUser match {
      case Nil => "no plays recorded"
      case (topUser, total) :: _ => s"top listener $topUser with ${total}s played"
    }

    println(s"[SCALA-HELLO] user totals -> ${byUser.map { case (u, d) => s"$u=$d" }.mkString(", ")}")
    println(s"[SCALA-HELLO] song counts  -> $genreCounts")
    println(s"[SCALA-HELLO] $headline")

    spark.stop()
    println("[SCALA-HELLO] OK")
  }
}
