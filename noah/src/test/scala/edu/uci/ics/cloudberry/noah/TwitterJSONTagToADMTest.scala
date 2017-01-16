package edu.uci.ics.cloudberry.noah

import java.io.File

import edu.uci.ics.cloudberry.gnosis._
import org.scalatest.{FlatSpec, Matchers}
import edu.uci.ics.cloudberry.util.Profile._

class TwitterJSONTagToADMTest extends FlatSpec with Matchers {
//  "USGeoGnosis" should "tag the us json file" in {
//    val shapeMap = Map.apply(StateLevel -> new File("neo/public/data/state.json"),
//                             CountyLevel -> new File("neo/public/data/county.json"),
//                             CityLevel -> new File("neo/public/data/city.json"))
//    val usGeoGnosis = profile("load shapes")(new USGeoGnosis(shapeMap))
//    for (ln <- scala.io.Source.fromURL(getClass.getResource("/sample.json")).getLines()) {
//      TwitterJSONTagToADM.tagOneTweet(ln, usGeoGnosis)
//    }
//  }

  "USGeoGnosis" should "tag the us json file" in {
    val shapeMap = Map.apply(BrCountryLevel -> new File("neo/public/data/Br/BR_country.json"),
      BrStateLevel -> new File("neo/public/data/Br/BR_states.json"),
      BrCityLevel -> new File("neo/public/data/Br/BR_cities.json"))
    val brGeoGnosis = profile("load shapes")(new BrGeoGnosis(shapeMap))
    for (ln <- scala.io.Source.fromURL(getClass.getResource("/sampleBr.json")).getLines()) {
      TwitterJSONTagBrToADM.tagOneTweet(ln, brGeoGnosis)
    }
  }

}
