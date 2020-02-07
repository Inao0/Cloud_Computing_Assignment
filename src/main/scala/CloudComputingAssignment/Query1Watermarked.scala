package CloudComputingAssignment

import java.io.{File, FileWriter, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.TimeWindow
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}

object Query1Watermarked {
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

  val LATITUDE_CELL_1_1: Double = 41.474937
  val LATITUDE_STEP: Double = -0.004491556
  val LONGITUDE_CELL_1_1: Double = -74.913585
  val LONGITUDE_STEP: Double = 0.005986

  def computeLongitudeGridId(longitude: Double): Int = {
    Math.round((longitude - LONGITUDE_CELL_1_1) / LONGITUDE_STEP).toInt + 1
  }

  def computeLatitudeGridId(latitude: Double): Int = {
    Math.round((latitude - LATITUDE_CELL_1_1) / LATITUDE_STEP).toInt + 1
  }

  def gridCellOf(longitude: Double, latitude: Double): GridCell = {
    (computeLatitudeGridId(latitude), computeLongitudeGridId(longitude))
  }

  //TODO : Add freshness
  def getTopN(windowStateOfTaxiRoutes: WindowStateOfTaxiRoutes, n: Int): WindowTopN = {
    val result = new Array[((GridCell, GridCell), Int)](n)
    //val values = Map[(GridCell,GridCell),Int]()
    val values = collection.mutable.Map[(GridCell, GridCell), Int](windowStateOfTaxiRoutes.currentCount.toSeq: _*) //windowStateOfTaxiRoutes.currentCount.clone().asInstanceOf[Map[(GridCell, GridCell), Int]]
    /*println("\n ======================= ")
    println("\n Get top N on non-empty : " + windowStateOfTaxiRoutes.currentCount.nonEmpty)
    println("\n ======================= \n")*/
    for (i <- 0 until n) {
      //result(i)=
      if (values.nonEmpty) {
        result(i) = values.maxBy { case (key, value) => value }
        values.-=(result(i)._1)
      } else {
        result(i) = (((-1, -1), (-1, -1)), -1)
      }
    }
    WindowTopN(windowStateOfTaxiRoutes.time_window, result)
  }

  def main(args: Array[String]) {

    val WINDOW: String = "30 minutes"
    val SLIDE: String = "5 minutes"
    val DELAY: String = "1 minutes"

    val spark = SparkSession.builder()
      .appName("CloudComputingAssignment")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sqlContext.sql("set spark.sql.shuffle.partitions=200")

    import spark.sqlContext.implicits._

    //val latitudeGrid_udf = udf((latitude: Double) => computeLatitudeGridId(latitude))
    //val longitudeGrid_udf = udf((latitude: Double) => computeLongitudeGridId(latitude))

    val taxiRidesFile = "data/taxiRides/"
    val schema = ScalaReflection.schemaFor[NYCTaxiRides].dataType.asInstanceOf[StructType]
    val taxiRides = spark.readStream.format("csv")
      .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(schema)
      .load(taxiRidesFile)
    val taxiRides2 = taxiRides.select("pickup_datetime", "dropoff_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude")
    taxiRides2.printSchema()
    val taxiRides3 = taxiRides2
      .withColumn("time_window", window(col("dropoff_datetime"), WINDOW, SLIDE))
    taxiRides3.printSchema()
    val taxiRides4 = taxiRides3
      //.filter($"pickup_latitude" > LATITUDE_CELL_1_1-0.5*LATITUDE_STEP && $"pickup_latitude" < LATITUDE_CELL_1_1 + 299.5*LATITUDE_STEP && $"pickup_longitude" >  LATITUDE_CELL_1_1-0.5*LATITUDE_STEP && $"pickup_longitude" <  LATITUDE_CELL_1_1+299.5*LATITUDE_STEP)
      //.filter($"dropoff_latitude" >  LONGITUDE_STEP-0.5*LONGITUDE_STEP && $"dropoff_latitude" < LONGITUDE_STEP+299.5*LONGITUDE_STEP && $"dropoff_longitude" > LONGITUDE_STEP-0.5*LONGITUDE_STEP && $"dropoff_longitude" < LONGITUDE_STEP+299.5*LONGITUDE_STEP)
      .map(row => NYCTaxiRidesRefined(
        (row.getStruct(6).get(0).asInstanceOf[java.sql.Timestamp], row.getStruct(6).get(1).asInstanceOf[java.sql.Timestamp]), //.asInstanceOf[MyTimeWindow]
        gridCellOf(row.getDouble(2), row.getDouble(3)),
        gridCellOf(row.getDouble(4), row.getDouble(5)),
        row.get(1).asInstanceOf[java.sql.Timestamp]))
      .filter($"pickup_gridCell._1" > 0 && $"pickup_gridCell._1" < 301 && $"pickup_gridCell._2" > 0 && $"pickup_gridCell._2" < 301)
      .filter($"dropoff_gridCell._1" > 0 && $"dropoff_gridCell._1" < 301 && $"dropoff_gridCell._2" > 0 && $"dropoff_gridCell._2" < 301)
    //      .map(row => NYCTaxiRidesRefined(row.get(7).asInstanceOf[MyTimeWindow], gridCellOf(row.getDouble(2), row.getDouble(3)), gridCellOf(row.getDouble(4), row.getDouble(5)), row.get(1).asInstanceOf[java.sql.Timestamp]))

    /*   taxiRides2
       .withColumn("pickup_latitude", latitudeGrid_udf(col("pickup_latitude")))
       .withColumn("pickup_longitude", longitudeGrid_udf(col("pickup_longitude")))
       .withColumn("dropoff_latitude", latitudeGrid_udf(col("dropoff_latitude")))
       .withColumn("dropoff_longitude", longitudeGrid_udf(col("dropoff_longitude")))
       .withColumn("time_window", window(col("dropoff_datetime"), WINDOW, SLIDE))
       .filter($"pickup_latitude" > 0 && $"pickup_latitude" < 301 && $"pickup_longitude" > 0 && $"pickup_longitude" < 301)
       .filter($"dropoff_latitude" > 0 && $"dropoff_latitude" < 301 && $"dropoff_longitude" > 0 && $"dropoff_longitude" < 301)*/
    taxiRides4.printSchema()
    //val writer = taxiRides.groupBy("medallion").count().writeStream.format("console").outputMode("complete").start()
    //val checkpointDir = "./checkpoint"
    /*val taxiRides4 = taxiRides3
      .groupBy("time_window", "pickup_gridCell", "dropoff_gridCell")
      .agg(max("latest_dropoff").as("latest_dropoff"))
    //.orderBy(asc("time_window"), desc("count"))
    taxiRides4.printSchema()*/

    println("Number of shuffle paritition" + spark.conf.get("spark.sql.shuffle.partitions"))

    val taxiRides5 = taxiRides4
      .withWatermark("latest_dropoff", "2 minutes")
      .as[NYCTaxiRidesRefined]
      .groupByKey(rides => rides.time_window).mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(flatMappingFunction)
    //.groupByKey(rides => rides.time_window).flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.EventTimeTimeout())(flatMappingFunction)


    val writerForText: ForeachWriter[WindowTopN] = new ForeachWriter[WindowTopN] {
      var fileWriter: FileWriter = _

      def open(partitionId: Long, version: Long): Boolean = {
        println("open : partition" + partitionId)
        FileUtils.forceMkdir(new File(s"./logStream/${partitionId}"))
        fileWriter = new FileWriter(new File(s"./logStream/${partitionId}/temp"), true)
        fileWriter.append("\n \n Opened \n")
        true
        // open connection
      }

      def close(errorOrNull: Throwable): Unit = {
        fileWriter.close()
      }

      def process(value: WindowTopN): Unit = {
        println("process " + value.time_window.toString() + value.currentCount.mkString(";"))
        //fileWriter.append("TOTOTOTOTOTO ======================== \n")
        fileWriter.append(value.time_window.toString() + value.currentCount.mkString(";") + "\n")
      }
    }

    taxiRides5.coalesce(1).writeStream
      .outputMode(OutputMode.Update())
      .foreach(writerForText)
      .option("checkpointLocation", "./checkpoint")
      .start()
      .awaitTermination()



    /*
        val query = sessionUpdates
          .writeStream
          .outputMode("update")
          .foreach(writerForText)
          .start()*/
    /*  val writer2 = taxiRides4.writeStream
      .outputMode("complete")
      .option("truncate", "false")
      .foreachBatch {
        (batchDs: Dataset[Row], batchId: Long) =>
          val topCountByWindowAndStateDf = batchDs
            .withColumn("rank", rank().over(Window.partitionBy("time_window").orderBy(batchDs.col("count").desc)))
            .orderBy("time_window")
            .filter(col("rank") <= 3)
            .drop("rank")
            .collect().toList

          val fileWriter = new FileWriter("./log.txt", false)
          val printWriter = new PrintWriter(fileWriter)
          printWriter.println(topCountByWindowAndStateDf.mkString("\n"))
          fileWriter.close()
          println(s"Batch: $batchId processed \n")

      }.start()
    writer2.awaitTermination()*/
  }

  import org.apache.spark.sql.streaming.GroupState

  def flatMappingFunction(key: MyTimeWindow, values: Iterator[NYCTaxiRidesRefined], state: GroupState[WindowStateOfTaxiRoutes]):
  WindowTopN = {
    if (state.hasTimedOut) {

      // when the state has a timeout, the values are empty
      // this validation is only to illustrate that point
      assert(values.isEmpty, "When the state has a timeout, the values are empty")
      val result = getTopN(state.get, 3)
      // evict the timed-out state
      state.remove()
      // emit the result of transforming the current state into an output record
      //         result

      result
    } else {
      // get current state or create a new one if there's no previous state

      val currentState = state.getOption.getOrElse(new WindowStateOfTaxiRoutes(key, Map[(GridCell, GridCell), Int]()))
      // enrich the state with the new events
      val updatedCount = collection.mutable.Map[(GridCell, GridCell), Int](currentState.currentCount.toSeq: _*)
      values.foreach(ride => {
        updatedCount((ride.pickup_gridCell, ride.dropoff_gridCell)) = updatedCount.getOrElse((ride.pickup_gridCell, ride.dropoff_gridCell), 0) + 1
      })
      // update the state with the enriched state
      //println("MAJ count : " + updatedCount.nonEmpty + "\n")
      val updatedState = WindowStateOfTaxiRoutes(key, collection.immutable.Map(updatedCount.toList: _*))
      state.update(updatedState)
      /*     val updatedState = values.foldLeft(currentState) { case (st, ev) => st.add(ev) }
           state.update(updatedState)
           state.setTimeoutDuration("30 seconds")
           // only when we have enough data, create a WeatherEventAverage from the accumulated state
           // before that, we return an empty result.
           stateToAverageEvent(key, updatedState)*/
      getTopN(updatedState, 3)
    }
  }
}

/*
import org.apache.spark.sql.streaming.GroupState
def flatMappingFunction(key: String, values: Iterator[WeatherEvent], state: GroupState[FIFOBuffer[WeatherEvent]]):
Iterator[WeatherEventAverage] = {
  if (state.hasTimedOut) {
  // when the state has a timeout, the values are empty
  // this validation is only to illustrate that point
  assert(values.isEmpty, "When the state has a timeout, the values are empty")
  val result = stateToAverageEvent(key, state.get)
  // evict the timed-out state
  state.remove()
  // emit the result of transforming the current state into an output record
  result
} else {
  // get current state or create a new one if there's no previous state
  val currentState = state.getOption.getOrElse(new FIFOBuffer[WeatherEvent](ElementCountWindowSize))
  // enrich the state with the new events
  val updatedState = values.foldLeft(currentState){case (st, ev) => st.add(ev)}
  // update the state with the enriched state
  state.update(updatedState)
  state.setTimeoutDuration("30 seconds")
  // only when we have enough data, create a WeatherEventAverage from the accumulated state
  // before that, we return an empty result.
  stateToAverageEvent(key, updatedState)
}
}*/