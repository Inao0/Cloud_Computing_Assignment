package CloudComputingAssignment


/** A processed NewYork City Taxi Ride
 *
 * @param time_window      : Time Window
 * @param pickup_gridCell  : pickup cell
 * @param dropoff_gridCell : drop-off cell
 * @param dropoff_time     : latest
 */
case class NYCTaxiRide(time_window: MyTimeWindow,
                       pickup_gridCell: GridCell,
                       dropoff_gridCell: GridCell,
                       dropoff_time: java.sql.Timestamp,
                      )

