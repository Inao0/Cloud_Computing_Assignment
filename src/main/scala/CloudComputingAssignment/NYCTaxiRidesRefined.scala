package CloudComputingAssignment


//taxiRides3.groupBy("time_window", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude").agg(max("dropoff_datetime").as("latest_dropoff"),count("time_window").as("count"))//.orderBy(asc("time_window"), desc("count"),desc("latest_dropoff"))

case class NYCTaxiRidesRefined(time_window: MyTimeWindow,
                               pickup_gridCell : GridCell,
                               dropoff_gridCell : GridCell,
                               latest_dropoff: java.sql.Timestamp,
                              )

/*The collection libraries have a uniform approach to equality and hashing. The idea is, first, to divide collections into sets, maps, and sequences. Collections in different categories are always unequal. For instance, Set(1, 2, 3) is unequal to List(1, 2, 3) even though they contain the same elements. On the other hand, within the same category, collections are equal if and only if they have the same elements (for sequences: the same elements in the same order). For example, List(1, 2, 3) == Vector(1, 2, 3), and HashSet(1, 2) == TreeSet(2, 1).
Example line from data file :
07290D3599E7A0D62097A346EFCC1FB5  medallion 	an md5sum of the identifier of the taxi - vehicle bound
E7750A37CAB07D0DFF0AF7E3573AC141  hack_license 	an md5sum of the identifier for the taxi license
2013-01-01 00:00:00               pickup_datetime 	time when the passenger(s) were picked up
2013-01-01 00:02:00               dropoff_datetime 	time when the passenger(s) were dropped off
120                               trip_time_in_secs 	duration of the trip
0.44                              trip_distance 	trip distance in miles
-73.956528                        pickup_longitude 	longitude coordinate of the pickup location
40.716976                         pickup_latitude 	latitude coordinate of the pickup location
-73.962440                        dropoff_longitude 	longitude coordinate of the drop-off location
40.715008                         dropoff_latitude 	latitude coordinate of the drop-off location
CSH                               payment_type 	the payment method - credit card or cash
3.50                              fare_amount 	fare amount in dollars
0.50                              surcharge 	surcharge in dollars
0.50                              mta_tax 	tax in dollars
0.00                              tip_amount 	tip in dollars
0.00                              tolls_amount 	bridge and tunnel tolls in dollars
4.50                              total_amount 	total paid amount in dollars
*/