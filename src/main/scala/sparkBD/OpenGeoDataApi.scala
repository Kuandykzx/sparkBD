package sparkBD
import sttp.client4.quick._

case class Geoposition(lat: Double, lng: Double)

object OpenGeoDataApi {
  private val API_KEY = sys.env.getOrElse("OPEN_DATA_API_KEY", "testKey")
  // System environment should contain API Key, else to test this need to put manually
  def getLatAndLongForAddress(address: String): Option[Geoposition] = {
    val params = Map(
      "q" -> address,
      "key" -> API_KEY
    )

    val response = quickRequest
      .get(uri"https://api.opencagedata.com/geocode/v1/json?$params")
      .auth.bearer(API_KEY)
      .header("Content-Type", "application/json")
      .send()

    // sends request to open cage data to fetch geo details

    val json = ujson.read(response.body)
    for {
      result <- json("results").arrOpt.flatMap(_.headOption)
      geo <- result("geometry").objOpt
      lat <- geo.get("lat").flatMap(_.numOpt)
      lng <- geo.get("lng").flatMap(_.numOpt)
    } yield Geoposition(lat, lng)

    // we fet geo position (lat, lng) details from api
  }
}
