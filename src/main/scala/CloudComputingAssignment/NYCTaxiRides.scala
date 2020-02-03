package CloudComputingAssignment

import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.TimestampType

case class NYCTaxiRides(medallion: String,
                        hack_license: String,
                        pickup_datetime: java.sql.Timestamp,
                        dropoff_datetime: java.sql.Timestamp,
                        trip_time_in_secs: Int,
                        trip_distance: Float,
                        pickup_longitude : Double,
                        pickup_latitude : Double,
                        dropoff_longitude : Double,
                        dropoff_latitude : Double,
                        payment_type : String,
                        fare_amount: Float,
                        surcharge: Float,
                        mta_tax: Float,
                        tip_amount: Float,
                        tolls_amount: Float,
                        total_amount: Float)


/*
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