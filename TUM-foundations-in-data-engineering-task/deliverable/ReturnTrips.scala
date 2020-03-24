import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {
  def distFunc = udf((lat_a: Double, lon_a: Double, lat_b: Double, lon_b: Double) => {
    val R = 6371000
    
    val latDist = lat_a - lat_b
    val lngDist = lon_a - lon_b
    val sinLat = Math.sin(latDist / 2)
    val sinLng = Math.sin(lngDist / 2)

    val a = sinLat * sinLat + (Math.cos(lat_a) * Math.cos(lat_b) * sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val res = (R * c)
    res
  })

  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {
    import spark.implicits._

    val latBucket = 36000
    val timeBucket = 8 * 60 * 60

    val tripsBucks = trips.select("pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude",
                                  "tpep_pickup_datetime", "tpep_dropoff_datetime")
                          .withColumn("pickup_latitude", toRadians($"pickup_latitude"))
                          .withColumn("pickup_longitude", toRadians($"pickup_longitude"))
                          .withColumn("dropoff_latitude", toRadians($"dropoff_latitude"))
                          .withColumn("dropoff_longitude", toRadians($"dropoff_longitude"))
                          .withColumn("pickup_location_bucket", floor($"pickup_latitude" * latBucket))
                          .withColumn("dropoff_location_bucket", floor($"dropoff_latitude" * latBucket))
                          .withColumn("pickup_time_bucket", floor(($"tpep_pickup_datetime".cast("double")) / timeBucket))
                          .withColumn("dropoff_time_bucket", floor(($"tpep_dropoff_datetime".cast("double")) / timeBucket))
                          .withColumn("tpep_pickup_datetime", $"tpep_pickup_datetime".cast("double"))
                          .withColumn("tpep_dropoff_datetime", $"tpep_dropoff_datetime".cast("double"))

    val tripsNeighbors = tripsBucks.withColumn("pickup_location_bucket", 
                                                    explode(array($"pickup_location_bucket" - 1,
                                                                  $"pickup_location_bucket",
                                                                  $"pickup_location_bucket" + 1)))
                                    .withColumn("dropoff_location_bucket",
                                                    explode(array($"dropoff_location_bucket" - 1,
                                                                  $"dropoff_location_bucket", 
                                                                  $"dropoff_location_bucket" + 1)))
                                    .withColumn("pickup_time_bucket", 
                                                    explode(array($"pickup_time_bucket" - 1,
                                                                  $"pickup_time_bucket")))

    val joinedTrips = tripsBucks.as("a").join(tripsNeighbors.as("b"),
        ($"b.dropoff_location_bucket" === $"a.pickup_location_bucket") &&
        ($"b.pickup_time_bucket" === $"a.dropoff_time_bucket") &&
        ($"b.pickup_location_bucket" === $"a.dropoff_location_bucket"))
        .withColumn("pickup_dropoff_dist", distFunc($"a.pickup_latitude", $"a.pickup_longitude", 
                                                    $"b.dropoff_latitude", $"b.dropoff_longitude"))
        .withColumn("dropoff_pickup_dist", distFunc($"a.dropoff_latitude", $"a.dropoff_longitude",
                                                    $"b.pickup_latitude", $"b.pickup_longitude"))

    val finalTrips = joinedTrips.filter(
        ($"b.tpep_pickup_datetime" > $"a.tpep_dropoff_datetime") &&
        (($"b.tpep_pickup_datetime" - $"a.tpep_dropoff_datetime") < timeBucket) &&
        ($"dropoff_pickup_dist" < dist) &&
        ($"pickup_dropoff_dist" < dist))
            
    finalTrips
  }
}
