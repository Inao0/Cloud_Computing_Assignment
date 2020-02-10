package CloudComputingAssignment

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** Provides a solution for parsing DEBS 2015 csv data files by using the implemented csv reader
 *
 * This implementation doesn't handle properly streaming data
 * Each new file added in the input folder will be processed independently from the previous ones
 * Therefore if several elements belonging to the same window are part of different files added one
 * after the other, this application will create a top N per file per window.
 *
 * This solution output one row per member of the top N in the output files. Empty rank do not appear.
 *
 * For example :
 *
 * &#91;&#91;time_window],pickup_latitude,pickup_longitude,dropoff_latitude,dropoff_longitude,latest_dropoff,count,rank
 *
 * &#91;&#91;2012-12-31 23:41:00.0,2013-01-01 00:11:00.0],1,1,1,300,2013-01-01 00:10:30.0,3,1]
 * &#91;&#91;2012-12-31 23:41:00.0,2013-01-01 00:11:00.0],1,1,300,1,2013-01-01 00:10:00.0,3,2]
 *
 * The default output file is /Result/Batch.txt
 */
object Query1_Batch {
  /**
   * Folder for output files
   */
  val OUTPUT_FILE: String = RESULT_FOLDER + "/Batch.txt"

  /** Main program - process DEBS 2015  NYC Taxi rides datafiles to solve a problem inspired by the first query of the challenge
   *
   * Loop that is executed when the class name is passed as argument alongside to the jar file to the spark-submit script
   * See README.md for more information about how to run this application
   *
   */
  def main(args: Array[String]) {


    /*Initialising spark context*/
    val spark = SparkSession.builder()
      .appName("CloudComputingAssignment")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*Import implicits_ allows sparks to handle lots of the conversions automatically.
    * see sparks Encoders for more information */
    import spark.sqlContext.implicits._

    /*Read as a Stream the input files according to rawTaxiRidesStructType*/
    val rawTaxiRides = spark.readStream.format("csv")
      .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(rawTaxiRidesStructType)
      .load(INPUT_FOLDER)

    val taxiRides = rawTaxiRides
      // Remove unnecessary columns
      .select("dropoff_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude")
      // Transform latitude and longitude doubles into grid cells id
      .withColumn("pickup_latitude", latitudeGrid_udf($"pickup_latitude"))
      .withColumn("pickup_longitude", longitudeGrid_udf($"pickup_longitude"))
      .withColumn("dropoff_latitude", latitudeGrid_udf($"dropoff_latitude"))
      .withColumn("dropoff_longitude", longitudeGrid_udf($"dropoff_longitude"))
      //Filter out outliers rides having pickups are drop-off areas outside the 300x300 grid
      .filter($"pickup_latitude" > 0 && $"pickup_latitude" < 301 && $"pickup_longitude" > 0 && $"pickup_longitude" < 301)
      .filter($"dropoff_latitude" > 0 && $"dropoff_latitude" < 301 && $"dropoff_longitude" > 0 && $"dropoff_longitude" < 301)
      //Add a sliding time window column
      .withColumn("time_window", window($"dropoff_datetime", WINDOW, SLIDE))
    taxiRides.printSchema()

    // Group taxi rides by time window and routes and create to aggregation columns : one with the count and one with the latest drop-off datetime
    val RoutesCountAndLatestDropoffPerWindow = taxiRides.groupBy("time_window", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude").agg(max("dropoff_datetime").as("latest_dropoff"), count("time_window").as("count")) //.orderBy(asc("time_window"), desc("count"))

    val streamingQuery = RoutesCountAndLatestDropoffPerWindow
      .writeStream
      .outputMode("complete")
      .option("truncate", "false")
      /* Streaming dataset do not support some opperations
       Multiple streaming aggregations are not possible in spark streaming and sorting is only possible after an aggregation
      Therefore we have done both aggregation at the same time and we use the foreachBatch sink in order
      to apply additional modifications that allows to compute for each window the rank of every routes */
      .foreachBatch {
        (batchDs: Dataset[Row], batchId: Long) =>
          val topCountByWindowAndStateDf = batchDs
            //Add ranking column
            .withColumn("rank", rank().over(Window.partitionBy("time_window").orderBy(desc("count"), desc("latest_dropoff"))))
            //Keep only rows that have rank smaller than the TOP_SIZE
            .filter($"rank" <= 3)
            //Order first by time_window, then by rank
            .orderBy("time_window", "rank")
            //.drop("rank") // can be uncommented to drop the "rank" column
            .collect().toList // potential bottle neck + risk of OOM error

          // Set up the output and print the result of the batch
          val fileWriter = new FileWriter(OUTPUT_FILE, false) // switch to true if you want to append the ouput file
          val printWriter = new PrintWriter(fileWriter)
          printWriter.println(topCountByWindowAndStateDf.mkString("\n"))
          fileWriter.close()
          println(s"Batch: $batchId processed \n")
      }.start()

    streamingQuery.awaitTermination()
  }
}
