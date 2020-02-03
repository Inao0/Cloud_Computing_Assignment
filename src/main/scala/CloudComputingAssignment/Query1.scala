package CloudComputingAssignment

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, SparkSession}


object Query1 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("CloudComputingAssignment")
      .getOrCreate()
    import spark.implicits._;

    val taxiRidesFile =  "data/taxiRides/short_sorted_data.csv"
    //val taxiRidesDs = spark.read.csv(taxiRidesFile)

    val schema = ScalaReflection.schemaFor[NYCTaxiRides].dataType.asInstanceOf[StructType]
    val taxiRidesDs = spark
      .read.format("csv")
      .option("header","false")
      .option("separator",',')
      .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(schema)
      .load(taxiRidesFile)
      .as[NYCTaxiRides]
    taxiRidesDs.printSchema()
    taxiRidesDs.show()

    /*

    /* map */
    var map = sc.textFile("data/taxiRides/sorted_data.csv").flatMap(line => line.split(",")).map(word => (word,1))

    /* reduce */
    var counts = map.reduceByKey(_ + _)

    /* print */
    counts.collect().foreach(println)

    /* or save the output to file */
    counts.saveAsTextFile("out.txt")

    sc.stop()*/
  }
}
