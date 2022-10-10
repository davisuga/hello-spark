package helloworld

import org.apache.spark.sql.SparkSession

// object HelloWorld:

@main def run =
  val spark = SparkSession.builder
    .appName("HelloWorld")
    .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
    .getOrCreate() // 1
  import spark.implicits._ // 2

  val df = List("hello", "world").toDF // 3
  df.show() // 4

  spark.stop
