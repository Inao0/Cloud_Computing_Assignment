package CloudComputingAssignment

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions._

object Query1 {

  /*
  Question 1: Is the earth flat or how to map coordinates to cells?
  Answer: For the challenge we allow a simplified flat earth assumption for mapping coordinates to cells
  in the queries. You can assume that a distance of 500 meter south corresponds to a change of 0.004491556
  degrees in the coordinate system. For moving 500 meter east you can assume a change of 0.005986 degrees
  in the coordinate system.

  The cells for this query are squares of 500 m X 500 m. The cell grid starts with cell 1.1, located at
  41.474937, -74.913585 (in Barryville). The coordinate 41.474937, -74.913585 marks the center of the
  first cell. Cell numbers increase towards the east and south, with the shift to east being the first
  and the shift to south the second component of the cell, i.e., cell 3.7 is 2 cells east and 6 cells
  south of cell 1.1.
   */
  def computeLongitudeGridId(longitude: Double): Int = {
    Math.round((longitude + 74.913585) / 0.005986).toInt + 1
  }

  def computeLatitudeGridId(latitude: Double): Int = {
    Math.round((latitude - 41.474937) / -0.004491556).toInt + 1
  }

  def main(args: Array[String]) {

    val WINDOW: String = "30 minutes"
    val SLIDE: String = "10 minutes"

    val spark = SparkSession.builder()
      .appName("CloudComputingAssignment")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    val latitudeGrid_udf = udf((latitude: Double) => computeLatitudeGridId(latitude))
    val longitudeGrid_udf = udf((latitude: Double) => computeLongitudeGridId(latitude))
    val taxiRidesFile = "data/taxiRides/"
    val schema = ScalaReflection.schemaFor[NYCTaxiRides].dataType.asInstanceOf[StructType]
    val taxiRides = spark.readStream.format("csv")
      .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(schema)
      .load(taxiRidesFile)
    val taxiRides2 = taxiRides.select("pickup_datetime", "dropoff_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude")
    taxiRides2.printSchema()
    val taxiRides3 = taxiRides2
      .withColumn("pickup_latitude", latitudeGrid_udf(col("pickup_latitude")))
      .withColumn("pickup_longitude", longitudeGrid_udf(col("pickup_longitude")))
      .withColumn("dropoff_latitude", latitudeGrid_udf(col("dropoff_latitude")))
      .withColumn("dropoff_longitude", longitudeGrid_udf(col("dropoff_longitude")))
      .withColumn("time_window", window(col("dropoff_datetime"), WINDOW, SLIDE))
      .filter($"pickup_latitude" > 0 && $"pickup_latitude" < 301 && $"pickup_longitude" > 0 && $"pickup_longitude" < 301)
      .filter($"dropoff_latitude" > 0 && $"dropoff_latitude" < 301 && $"dropoff_longitude" > 0 && $"dropoff_longitude" < 301)
    taxiRides3.printSchema()
    //val writer = taxiRides.groupBy("medallion").count().writeStream.format("console").outputMode("complete").start()
    //val checkpointDir = "./checkpoint"
    val taxiRides4 = taxiRides3.groupBy("time_window", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude").agg(max("dropoff_datetime").as("latest_dropoff"),count("time_window").as("count"))//.orderBy(asc("time_window"), desc("count"))
    val writer2 = taxiRides4.writeStream
      .outputMode("complete")
      .option("truncate", "false")
      .foreachBatch {
        (batchDs: Dataset[Row], batchId: Long) =>
          val topCountByWindowAndStateDf = batchDs
            .withColumn("rank", rank().over(Window.partitionBy("time_window").orderBy(desc("count"),desc("latest_dropoff"))))
            .orderBy("time_window","rank")
            .filter(col("rank") <= 3)
            //.drop("rank")
            .collect().toList

          val fileWriter = new FileWriter("./log.txt", false)
          val printWriter = new PrintWriter(fileWriter)
          printWriter.println(topCountByWindowAndStateDf.mkString("\n"))
          fileWriter.close()
          println(s"Batch: $batchId processed \n")

      }.start()
    writer2.awaitTermination()
  }
}
