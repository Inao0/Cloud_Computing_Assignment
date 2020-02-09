package CloudComputingAssignment

/** State associated with a time window containing the count of drop-off and the most recent drop-off timestamp for
 * each routes used during the time window
 *
 * @param time_window  : associated time window
 * @param currentState : map of all the already used routes with their count and latest dropoff
 */
case class WindowStateOfTaxiRoutes(time_window: MyTimeWindow,
                                   currentState: Map[Route, CountAndLatestDropoff]
                                  )
