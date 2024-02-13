package sparkBD

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class DateField(year: String, month: String, day: String)
object SparkContextTest {
  private val FILE_NAMES = List(
    "part-00000-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
    "part-00001-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
    "part-00002-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
    "part-00003-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv",
    "part-00004-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
  )
  // file names of restaurant data

  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  // date formatter needed to partition parquet result

  private val GEOHASHING_PRECISION = 4
  // geo hashing precision

  val makeGeoHashFromLatLong: UserDefinedFunction = udf((lat: Double, lng: Double) => {
    GeoHash.geoHashStringWithCharacterPrecision(lat, lng, GEOHASHING_PRECISION)
  })
  // udf to create geohash from lat and lng

  val retrieveGeoposition: UserDefinedFunction = udf((name: String, country: String, city: String) => {
    OpenGeoDataApi.getLatAndLongForAddress(s"$name $country $city")
  })
  // udf to call open cage data api to fetch lat and lng details from address

  val getDateFields: UserDefinedFunction = udf({ date: String =>
    val localDate = LocalDate.parse(date, dateFormatter)
    DateField(localDate.getYear.toString, localDate.getMonthValue.toString, localDate.getDayOfMonth.toString)
  })
  // formatting date field in parquet data, to properly partition result

  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkContextTest")
      .getOrCreate()
    // creating spark context

    val files: List[String] = FILE_NAMES.map(str => s"src/main/resources/restaurant_csv/$str")
    val restaurant = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true").csv(files:_*)
    // reading restaurant data from files

    val restaurantsWithGeo = restaurant.filter(col("lng").isNotNull && col("lat").isNotNull)
    val restaurantsWithoutGeo = restaurant.filter(col("lng").isNull || col("lat").isNull)
      .drop("lat", "lng")
      .withColumn("geo", retrieveGeoposition(col("franchise_name"), col("country"), col("city")))
      .select("geo.*", "*")
      .drop("geo")
    // divide restaurant info into null and not null columns for lng and lat
    // for those without lng and lat, we use udf to call open cage data api

    val restaurantsProcessed = restaurantsWithGeo.union(restaurantsWithoutGeo)
    // we bring back that divided data and create geohash for them
    val restaurantsWithGeohash = restaurantsProcessed.withColumn("geohash", makeGeoHashFromLatLong(col("lat"), col("lng")))


    val InputPath = List(
      "src/main/resources/weather/*/*/*/*.parquet"
    ) // parquet data

    val weather = spark.read.parquet(InputPath:_*)
    val weatherWithGeohash = weather.withColumn("geohash", makeGeoHashFromLatLong(col("lat"), col("lng")))
      .withColumn("date", getDateFields(col("wthr_date")))
      .select("date.*", "*")
      .drop("date")
    // we create geohash and format date field to further partition it

    val leftOuterJoin = weatherWithGeohash
      .join(restaurantsWithGeohash, weatherWithGeohash("geohash") === restaurantsWithGeohash("geohash"), "left")
    // we do left join weather and restaurant data
    weatherWithGeohash.write.partitionBy("year", "month", "day").parquet("src/main/resources/result")
  }
}