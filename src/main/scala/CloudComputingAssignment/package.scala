import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType

/** Provides classes for solving a problem inspired by the first query of the DEBS 2015 challenge
 */
package object CloudComputingAssignment {

  // type aliases
  type MyTimeWindow = (java.sql.Timestamp, java.sql.Timestamp)
  type GridCell = (Int, Int)
  type Route = (GridCell, GridCell)

  /*Create a rawTaxiRidesStructType from RawNYCTaxiRides to parse the inputs*/
  val rawTaxiRidesStructType: StructType = ScalaReflection.schemaFor[RawNYCTaxiRides].dataType.asInstanceOf[StructType]

  /**
   * Folder that is processed by the spark application
   */
  val INPUT_FOLDER = "data/taxiRides/"

  /**
   * Folder for result files
   */
  val RESULT_FOLDER = "./Result"

  /** Parameters of the sliding window and the watermark
   *
   * The delay value define how long the state of a sliding window state is kept in memory
   * in this application the time is based on the latest dropoff column which is suposed to
   * be always increasing.
   *
   * cf *Introducing mapGroupsWithState* in https://blog.yuvalitzchakov.com/exploring-stateful-streaming-with-spark-structured-streaming/
   * and : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
   */
  val WINDOW: String = "30 minutes"
  val SLIDE: String = "1 minutes"
  val DELAY: String = "1 minutes" // as values are supposed to arrive in order we can apply a really small watermark delay

  /**
   * Size of Top created per time window
   */
  val TOP_SIZE: Int = 3

  /** Justification for the following values come from https://debs.org/grand-challenges/2015/ :
   * Question 1: Is the earth flat or how to map coordinates to cells?
   * Answer: For the challenge we allow a simplified flat earth assumption for mapping coordinates to cells
   * in the queries. You can assume that a distance of 500 meter south corresponds to a change of 0.004491556
   * degrees in the coordinate system. For moving 500 meter east you can assume a change of 0.005986 degrees
   * in the coordinate system.
   * *
   * The cells for this query are squares of 500 m X 500 m. The cell grid starts with cell 1.1, located at
   * 41.474937, -74.913585 (in Barryville). The coordinate 41.474937, -74.913585 marks the center of the
   * first cell. Cell numbers increase towards the east and south, with the shift to east being the first
   * and the shift to south the second component of the cell, i.e., cell 3.7 is 2 cells east and 6 cells
   * south of cell 1.1. */
  val LATITUDE_CELL_1_1: Double = 41.474937
  val LATITUDE_STEP: Double = -0.004491556
  val LONGITUDE_CELL_1_1: Double = -74.913585
  val LONGITUDE_STEP: Double = 0.005986

  /** returns the index of the grid cell for a given longitude
   *
   * @param longitude longitude
   * @return corresponding grid cell index
   */
  def computeLongitudeGridId(longitude: Double): Int = {
    Math.round((longitude - LONGITUDE_CELL_1_1) / LONGITUDE_STEP).toInt + 1
  }

  /** returns the index of the grid cell for a given latitude
   *
   * @param latitude latitude
   * @return corresponding grid cell index
   */
  def computeLatitudeGridId(latitude: Double): Int = {
    Math.round((latitude - LATITUDE_CELL_1_1) / LATITUDE_STEP).toInt + 1
  }

  /** returns GridCell corresponding to the given longitude and latitude
   *
   * @param longitude longitude
   * @param latitude  latitude
   * @return corresponding GridCell, ie (latitude index, longitude ondex)
   */
  def gridCellOf(longitude: Double, latitude: Double): GridCell = {
    (computeLatitudeGridId(latitude), computeLongitudeGridId(longitude))
  }

  /*Create user define function that can be applied to latitude and longitude columns to turn double into grid cells id*/
  val latitudeGrid_udf: UserDefinedFunction = udf((latitude: Double) => computeLatitudeGridId(latitude))
  val longitudeGrid_udf: UserDefinedFunction = udf((latitude: Double) => computeLongitudeGridId(latitude))


}
