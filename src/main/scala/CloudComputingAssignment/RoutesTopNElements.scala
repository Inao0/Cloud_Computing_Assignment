package CloudComputingAssignment

/** An element of the Top N of most frequent routes in a given time window
 *
 * @param route                 : route
 * @param countAndLatestDropoff : number of drop-off and most recent drop-off timestamp during the time window
 */
case class RoutesTopNElements(route: Route,
                              countAndLatestDropoff: CountAndLatestDropoff)
