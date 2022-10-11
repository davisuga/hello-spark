package helloworld

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
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

  numbers.show()
  spark.stop
