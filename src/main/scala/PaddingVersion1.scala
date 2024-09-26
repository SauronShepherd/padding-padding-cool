import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object PaddingVersion1 {

  val numRows = 16000000
  val initChar = "+"
  val padChar = "-"
  val maxChars = 100

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create and populate a new dataframe
    val startPopulating = System.currentTimeMillis()
    val plusList = Seq.fill(numRows)(initChar)
    val populatedDf = plusList.toDF("str")
    val timePopulating = System.currentTimeMillis() - startPopulating

    // Define the UDF to pad a string with a character on both sides
    val padStringUDF = udf((input: String, length: Int) => {
      val totalPadding = length - input.length
      val paddingLeft = totalPadding / 2
      val paddingRight = totalPadding - paddingLeft
      padChar * paddingLeft + input + padChar * paddingRight
    })

    // Apply the padding UDF, replacing the existing column
    val paddedDf = populatedDf.withColumn("str", padStringUDF(col("str"), lit(maxChars)))

    // Show the rows
    val startPadding = System.currentTimeMillis()
    paddedDf.show(truncate=false)
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
