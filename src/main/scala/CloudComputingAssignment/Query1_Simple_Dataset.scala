package CloudComputingAssignment

import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Provides a solution for parsing DEBS 2015 csv data files by using the implemented csv reader
 *
 * This implementation doesn't handle streaming data.
 * This solution output one row per member of the top N in the output files.
 * For example :
 * time_window,pickup_latitude,pickup_longitude,dropoff_latitude,dropoff_longitude,latest_dropoff,count,rank
 * "[2012-12-31 23:41:00,2013-01-01 00:11:00]",1,1,1,300,2013-01-01T00:10:30.000+01:00,3,1
 * "[2012-12-31 23:41:00,2013-01-01 00:11:00]",1,1,300,1,2013-01-01T00:10:00.000+01:00,3,2
 *
 * The default output folder is /Result/Dataset
 */
object Query1_Simple_Dataset {
  /** Turn the time window column into a string column
   *
   * Implemented due to the following error
   * "CSV data source does not support struct<start:timestamp,end:timestamp> data type."
   *
   * @param c time window column to turn into string column
   * @return column of string
   */
  def stringifyTimeWindow(c: Column): Column = {
    concat(lit("["), lit(c.getField("start")), lit(","), lit(c.getField("end")), lit("]"))
  }

  /** Main program - process DEBS 2015  NYC Taxi rides datafiles to solve a problem inspired by the first query of the challenge
   *
   * Loop that is executed when the class name is passed as argument alongside to the jar file to the spark-submit script
   * See README.md for more information about how to run this application
   *
   */
  def main(args: Array[String]) {

    /**
     * Folder for output files
     */
    val OUTPUT_FOLDER: String = RESULT_FOLDER + "/Dataset"


    /*Initialising spark context*/
    val spark = builder()
      .appName("CloudComputingAssignment-Dataset")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*Import implicits_ allows sparks to handle lots of the conversions automatically.
    * see sparks Encoders for more information */
    import spark.sqlContext.implicits._

    /*Parse the input files according to rawTaxiRidesStructType*/
    val rawTaxiRidesDf = spark.read.format("csv")
      .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(rawTaxiRidesStructType)
      .load(INPUT_FOLDER)

    val taxiRides = rawTaxiRidesDf
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
    val RoutesCountAndLatestDropoffPerWindow = taxiRides.groupBy("time_window", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude").agg(max("dropoff_datetime").as("latest_dropoff"), count("time_window").as("count")) //.orderBy(asc("time_window"), desc("count"),desc("latest_dropoff"))
    RoutesCountAndLatestDropoffPerWindow.printSchema()

    val orderedTopNForAllWindows = RoutesCountAndLatestDropoffPerWindow
      //Adding a rank over window partition column computed with first criterion being the number of drop-off in the time window and 2nd criterion being the latest drop-off datetime
      .withColumn("rank", rank().over(Window.partitionBy("time_window").orderBy(desc("count"), desc("latest_dropoff"))))
      //Keep only rows that have rank smaller than the TOP_SIZE
      .filter($"rank" <= TOP_SIZE)
      //Order first by time_window, then by rank
      .orderBy("time_window", "rank")
    //.drop("rank") // can be uncommented to drop the "rank" column

    // turn the time window column into a string
    val topNWtihPrintableWindow = orderedTopNForAllWindows.withColumn("time_window", stringifyTimeWindow($"time_window")) //.write.text("log2.txt")
    topNWtihPrintableWindow.printSchema()

    //output the result as csv files
    spark.time(topNWtihPrintableWindow.write.option("header", "true").csv(OUTPUT_FOLDER))
  }
}
