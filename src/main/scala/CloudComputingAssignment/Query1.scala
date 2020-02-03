package CloudComputingAssignment

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, SparkSession}
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

  /*def computeGrid1Positions(ride: RawNYCTaxiRides): NYCTaxiRides = {
    val pickup_longitude_grid = computeLongitudeGridId(ride.pickup_longitude)
    val pickup_latitude_grid = computeLatitudeGridId(ride.pickup_latitude)
    val dropoff_longitude_grid = computeLongitudeGridId(ride.dropoff_longitude)
    val dropoff_latitude_grid = computeLatitudeGridId(ride.dropoff_latitude)*
    *
    NYCTaxiRides(ride.pickup_datetime, ride.dropoff_datetime, pickup_longitude_grid, pickup_latitude_grid, dropoff_longitude_grid, dropoff_latitude_grid)
  }*/

/*
    case class NYCTaxiRides(pickup_datetime: java.sql.Timestamp,
                        dropoff_datetime: java.sql.Timestamp,
                        pickup_longitude_grid: Int,
                        pickup_latitude_grid: Int,
                        dropoff_longitude_grid: Int,
                        dropoff_latitude_grid: Int,
                       )

    def computeGrid1Positions(ride: RawNYCTaxiRides): NYCTaxiRides = {
    val pickup_longitude_grid = computeLongitudeGridId(ride.pickup_longitude)
    val pickup_latitude_grid = computeLatitudeGridId(ride.pickup_latitude)
    val dropoff_longitude_grid = computeLongitudeGridId(ride.dropoff_longitude)
    val dropoff_latitude_grid = computeLatitudeGridId(ride.dropoff_latitude)
    NYCTaxiRides(ride.pickup_datetime, ride.dropoff_datetime, pickup_longitude_grid, pickup_latitude_grid, dropoff_longitude_grid, dropoff_latitude_grid)
  }*/

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("CloudComputingAssignment")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

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
    taxiRides3.printSchema()
    //val writer = taxiRides.groupBy("medallion").count().writeStream.format("console").outputMode("complete").start()
    //val checkpointDir = "./checkpoint"
    val writer2 = taxiRides3.groupBy("pickup_latitude","pickup_longitude","dropoff_latitude","dropoff_longitude").count().writeStream.format("console").outputMode("complete").start()
    writer2.awaitTermination()

    /* ========= VERSION PARSING INTO DATASET ===========
    import spark.implicits._;

    val taxiRidesFile =  "data/taxiRides/short_sorted_data.csv"
    //val taxiRidesDs = spark.read.csv(taxiRidesFile)

    val schema = ScalaReflection.schemaFor[NYCTaxiRides].dataType.asInstanceOf[StructType]
    val taxiRidesDs = spark
      .read.format("csv")
      .option("header","false")
      .option("separator",',')
      .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(schema)
      .load(taxiRidesFile)
      .as[NYCTaxiRides]
    taxiRidesDs.printSchema()
    taxiRidesDs.show()
    */
  }
}
