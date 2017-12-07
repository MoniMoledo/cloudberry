package edu.uci.ics.cloudberry.noah

import java.io.File

import edu.uci.ics.cloudberry.gnosis._
import org.scalatest.{FlatSpec, Matchers}
import edu.uci.ics.cloudberry.util.Profile._

class TwitterJSONTagToADMTest extends FlatSpec with Matchers {
  "USGeoGnosis" should "tag the us json file" in {
    val shapeMap = Map.apply(StateLevel -> new File("web/public/data/br_state.json"),
                             CountryLevel -> new File("web/public/data/br_country.json"),
                             CityLevel -> new File("web/public/data/br_city.json"))
    val usGeoGnosis = profile("load shapes")(new BrGeoGnosis(shapeMap))
    for (ln <- scala.io.Source.fromURL(getClass.getResource("/sample.json")).getLines()) {
      TwitterJSONTagToADM.tagOneTweet(ln, usGeoGnosis)
    }
  }

}
