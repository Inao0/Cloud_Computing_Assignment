package CloudComputingAssignment

/** A raw NewYork City taxi ride as found in data files
 *
 * This case clase is meant to provide spark a way to parse corresponding data files
 *
 * Example line from data file :
 *
 * 07290D3599E7A0D62097A346EFCC1FB5,E7750A37CAB07D0DFF0AF7E3573AC141,2013-01-01 00:00:00,2013-01-01 00:02:00,120,0.44,-73.956528,40.716976,-73.962440,40.715008,CSH,3.50,0.50,0.50,0.00,0.00,4.50
 *
 * where :
 * 07290D3599E7A0D62097A346EFCC1FB5  medallion 	an md5sum of the identifier of the taxi - vehicle bound
 * E7750A37CAB07D0DFF0AF7E3573AC141  hack_license 	an md5sum of the identifier for the taxi license
 * 2013-01-01 00:00:00               pickup_datetime 	time when the passenger(s) were picked up
 * 2013-01-01 00:02:00               dropoff_datetime 	time when the passenger(s) were dropped off
 * 120                               trip_time_in_secs 	duration of the trip
 * 0.44                              trip_distance 	trip distance in miles
 * -73.956528                        pickup_longitude 	longitude coordinate of the pickup location
 * 40.716976                         pickup_latitude 	latitude coordinate of the pickup location
 * -73.962440                        dropoff_longitude 	longitude coordinate of the drop-off location
 * 40.715008                         dropoff_latitude 	latitude coordinate of the drop-off location
 * CSH                               payment_type 	the payment method - credit card or cash
 * 3.50                              fare_amount 	fare amount in dollars
 * 0.50                              surcharge 	surcharge in dollars
 * 0.50                              mta_tax 	tax in dollars
 * 0.00                              tip_amount 	tip in dollars
 * 0.00                              tolls_amount 	bridge and tunnel tolls in dollars
 * 4.50                              total_amount 	total paid amount in dollars
 *
 * @param medallion         medallion 	an md5sum of the identifier of the taxi - vehicle bound
 * @param hack_license      hack_license 	an md5sum of the identifier for the taxi license
 * @param pickup_datetime   pickup_datetime 	time when the passenger(s) were picked up
 * @param dropoff_datetime  dropoff_datetime 	time when the passenger(s) were dropped off
 * @param trip_time_in_secs trip_time_in_secs 	duration of the trip
 * @param trip_distance     trip_distance 	trip distance in miles
 * @param pickup_longitude  pickup_longitude 	longitude coordinate of the pickup location
 * @param pickup_latitude   pickup_latitude 	latitude coordinate of the pickup location
 * @param dropoff_longitude dropoff_longitude 	longitude coordinate of the drop-off location
 * @param dropoff_latitude  dropoff_latitude 	latitude coordinate of the drop-off location
 * @param payment_type      payment_type 	the payment method - credit card or cash
 * @param fare_amount       fare_amount 	fare amount in dollars
 * @param surcharge         surcharge 	surcharge in dollars
 * @param mta_tax           mta_tax 	tax in dollars
 * @param tip_amount        tip_amount 	tip in dollars
 * @param tolls_amount      tolls_amount 	bridge and tunnel tolls in dollars
 * @param total_amount      total_amount 	total paid amount in dollars
 */
case class RawNYCTaxiRides(medallion: String,
                           hack_license: String,
                           pickup_datetime: java.sql.Timestamp,
                           dropoff_datetime: java.sql.Timestamp,
                           trip_time_in_secs: Int,
                           trip_distance: Float,
                           pickup_longitude: Double,
                           pickup_latitude: Double,
                           dropoff_longitude: Double,
                           dropoff_latitude: Double,
                           payment_type: String,
                           fare_amount: Float,
                           surcharge: Float,
                           mta_tax: Float,
                           tip_amount: Float,
                           tolls_amount: Float,
                           total_amount: Float)

