
import org.scalatest.flatspec.AnyFlatSpec
import sparkBD.{Geoposition, OpenGeoDataApi}

class SparkContextTestSpec extends AnyFlatSpec {
  val testAddress = "Frauenplan 1, 99423 Weimar, Germany"
  val testGeo: Geoposition = Geoposition(50.9775106, 11.3285424)

  "Open Geo Api" should "return lat and lng for address" in {
    assert(
      OpenGeoDataApi.getLatAndLongForAddress(testAddress)
        .contains(testGeo)
    )
  }

}
