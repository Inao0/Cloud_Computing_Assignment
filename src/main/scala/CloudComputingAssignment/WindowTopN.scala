package CloudComputingAssignment

/** Top N of the most frequent routes on a given time window
 *
 * @param time_window : time window
 * @param currentTop  : current top N as an array of TopNElement starting from the most frequent route
 * @param last_update : timestamp corresponding to the time at which this top has been updated
 */
case class WindowTopN(time_window: MyTimeWindow,
                      currentTop: Array[RoutesTopNElements],
                      last_update: java.sql.Timestamp
                     )
