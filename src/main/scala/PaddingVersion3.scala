import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object PaddingVersion3 {

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      //.config("spark.sql.adaptive.enabled","false")
      .getOrCreate()

    // Define constants
    val numRows = spark.sparkContext.defaultParallelism * 10000000
    val initChar = "+"
    val padChar = "-"
    val maxChars = 100

    // Create and populate a new dataframe
    val startPopulating = System.currentTimeMillis()
    val populatedDf = spark.range(numRows)
      .withColumn("str", lit(initChar))
      .drop("id")
    populatedDf.cache().count()
    val timePopulating = System.currentTimeMillis() - startPopulating

    // Define the UDF to pad a string with a character on both sides
    val padStringUDF = udf((input: String, length: Int) => {
      val totalPadding = length - input.length
      val paddingLeft = totalPadding / 2
      val paddingRight = totalPadding - paddingLeft
      padChar * paddingLeft + input + padChar * paddingRight
    })

    // Apply the padding UDF, replacing the existing column
    val startPadding = System.currentTimeMillis()
    val paddedDf = populatedDf.withColumn("str", padStringUDF(col("str"), lit(maxChars)))
    //paddedDf.filter("str is not null").count()
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
