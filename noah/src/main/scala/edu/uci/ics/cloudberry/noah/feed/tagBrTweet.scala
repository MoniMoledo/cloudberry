package edu.uci.ics.cloudberry.noah.feed

import java.io.File

import edu.uci.ics.cloudberry.gnosis._
import edu.uci.ics.cloudberry.noah.adm.{UnknownPlaceException, Tweet}
import edu.uci.ics.cloudberry.util.Profile._
import twitter4j.{TwitterException, TwitterObjectFactory}

object TagBrTweet {
  var shapeMap = Seq( BrCountryLevel -> "neo/public/data/br_country.json",
    BrStateLevel -> "neo/public/data/br_state.json",
    BrCityLevel -> "neo/public/data/br_city.json").toMap

  val brGeoGnosis = profile("loading resource") {
    new BrGeoGnosis(shapeMap.mapValues(new File(_)).toMap)
  }

  @throws[UnknownPlaceException]
  @throws[TwitterException]
  def tagOneTweet(ln: String, requireGeoField: Boolean): String = {
    val adm = Tweet.toBrADM(TwitterObjectFactory.createStatus(ln), brGeoGnosis, requireGeoField)
    return adm
  }
}
