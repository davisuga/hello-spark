package helloworld

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

@main def run =
  val spark = SparkSession.builder
    .appName("HelloWorld")
    .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
    .getOrCreate() // 1
  import spark.implicits._ // 2

  val numbers =
    Seq(5, 23, 66, 23, 654, 77, 35, 3455, 2333, 45, 234, 5555, 78, .2, 45, 2)
      .toDF("values")
      .withColumn("index", monotonicallyIncreasingId) // 3

  def merge(left: Dataset[Double], right: Dataset[Double]): Dataset[Double] =
    (left.isEmpty, right.isEmpty) match {
      case (false, true) => left
      case (true, false) => right
      case (false, false) =>
        val leftHead = left.first()
        val leftTail = left.tail(left.count().toInt - 1)
        val rightTail = right.tail(right.count().toInt - 1)
        val rightHead = right.first()
        //   case (Array(leftHead, leftTail*), Array(rightHead, rightTail*)) =>
        if (leftHead < rightHead)
          List(leftHead).toDS.join(merge(leftTail.toList.toDS, right))
        //   Array(leftHead, *)
        else List(rightHead).toDS.join(merge(left, rightTail.toList.toDS))
      // Array(rightHead, merge(left, rightTail.toArray)*)
    }

  def mergeSort(list: Dataset[Double]): Dataset[Double] = {
    val n = list.count() / 2

    if (n == 0) list // i.e. if list is empty or single value, no sorting needed
    else {
      val listValues = list.select("values")
      val (left, right) = (
        listValues.where($"index" > n),
        listValues.where($"index" < n)
      )
      import spark.implicits._ // 2

      merge(mergeSort(left), mergeSort(right))
    }
  }

  numbers.show()
  spark.stop
