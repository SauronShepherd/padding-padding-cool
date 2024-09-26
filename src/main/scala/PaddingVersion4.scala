import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object PaddingVersion4 {

  val numRows = 1000000000
  val initChar = "+"
  val padChar = "-"
  val maxChars = 100

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

    // Create and populate a new dataframe
    val startPopulating = System.currentTimeMillis()
    val populatedDf = spark.range(numRows)
      .withColumn("str", lit("+"))
      .drop("id")
    val timePopulating = System.currentTimeMillis() - startPopulating

    // Padding using built-in string functions, replacing the existing column
    val paddedDf = populatedDf.withColumn("str",
      expr(s"""LPAD(RPAD(str, LENGTH(str) + ($maxChars - LENGTH(str)) / 2, '$padChar'), $maxChars, '$padChar')""")
    )

    // Write the dataframe using no-op
    val startPadding = System.currentTimeMillis()
    paddedDf.write.format("noop").mode("overwrite").save()
    val timePadding = System.currentTimeMillis() - startPadding

    // Print final info
    println(s"numPartitions populatedDf: ${populatedDf.rdd.getNumPartitions}")
    println(s"numPartitions paddedDf: ${paddedDf.rdd.getNumPartitions}")
    println(s"numRows: $numRows - maxChars: $maxChars - timePopulating: $timePopulating - timePadding: $timePadding")

    // Wait for the user to press Enter
    println("Press Enter to continue...")
    scala.io.StdIn.readLine()

    // Stop Spark session
    spark.stop()

  }
}
