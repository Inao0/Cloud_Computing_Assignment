package CloudComputingAssignment

/** Contains a count and a latest drop-off timestamp
 * Meant to be corresponding to a timeWindow and Route
 *
 * Implements the ordered traits to be able to use the max function of scala to find the most frequented
 * route as defined by the debs 2015 challenge in the query 1 : the most of frequent route over a time
 * window is the one with the highest number of drop-off during the ime window. In case of equality in the
 * number of drop-off the second criterion is the most recent drop-off goes first.
 *
 * @param count          : number of drop-off (for a given route and time-window)
 * @param latest_dropoff : most recent drop-off (for a given route and time-window)
 */
case class CountAndLatestDropoff(count: Int,
                                 latest_dropoff: java.sql.Timestamp)
  extends Ordered[CountAndLatestDropoff] {

  /** Comparison operation used to find the most frequent route
   * The two ordering criteria are :
   * - first the highest count is seen as greater
   * - then in case of equality of count the most-recent drop-off is seen as greater
   *
   * @param that countAndLastetDropoff that should be compare to this instance
   * @return
   */
  override def compare(that: CountAndLatestDropoff): Int = {
    val compareCount = this.count.compareTo(that.count)
    if (compareCount == 0) return this.latest_dropoff.compareTo(that.latest_dropoff)
    compareCount
  }

  /* Tests for comparisons
    val a = CountAndLatestDropoff(1,new Timestamp(0L))
    val b = CountAndLatestDropoff(1,new Timestamp(0L))
    val c = CountAndLatestDropoff(1,new Timestamp(1L))
    val d = CountAndLatestDropoff(2,new Timestamp(0L))
    val e = CountAndLatestDropoff(2,new Timestamp(1L))
    println(a.compare(b)) //0
    println(a.compare(c)) //-1
    println(a.compare(d)) //-1
    println(a.compare(e)) //-1
    println(e.compare(c)) //1
    println(e.compare(d)) //1*/
}


