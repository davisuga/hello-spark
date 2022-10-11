package helloworld

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

trait Filterable[F[_]] {
  def filter[A](fa: F[A])(f: A => Boolean): F[A]
  def count[A](fa: F[A]): Long
}

object Filterable:
  given Filterable[RDD] with
    def filter[A](fa: RDD[A])(f: A => Boolean): RDD[A] = fa.filter(f)
    def count[A](fa: RDD[A]): Long = fa.count()
  given Filterable[List] with
    def filter[A](fa: List[A])(f: A => Boolean): List[A] = fa.filter(f)
    def count[A](fa: List[A]): Long = fa.length
  given Filterable[Seq] with
    def filter[A](fa: Seq[A])(f: A => Boolean): Seq[A] = fa.filter(f)
    def count[A](fa: Seq[A]): Long = fa.length

def countHaskellMentions[X[_]](
    content: X[String]
)(using container: Filterable[X]) = {
  container.count(
    container.filter(content)((page: String) => page.contains("Haskell"))
  )
}

@main def run =
  val spark = SparkSession.builder
    .appName("HelloWorld")
    .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
    .getOrCreate()
  import spark.implicits._

  val distributed: RDD[String] = ???
  val local: Seq[String] = ???

  val countDistributed = countHaskellMentions[RDD](distributed)
  val countLocal = countHaskellMentions[Seq](local)
  val countList = countHaskellMentions[List](local.toList)

  val numbers =
    Seq(5, 23, 66, 23, 654, 77, 35, 3455, 2333, 45, 234, 5555, 78, .2, 45, 2)
      .toDF("values")
      .withColumn("index", monotonicallyIncreasingId)

  val encyclopedia: RDD[String] = ???

  val result = encyclopedia.filter(_.contains("Haskell")).count()

  numbers.show()
  spark.stop
