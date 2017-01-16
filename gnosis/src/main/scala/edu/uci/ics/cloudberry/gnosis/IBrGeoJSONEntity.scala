package edu.uci.ics.cloudberry.gnosis

import java.nio.charset.Charset

import com.vividsolutions.jts.geom.Geometry
import play.api.libs.json.{JsObject, Json}

sealed trait IBrGeoJSONEntity extends IGeoJSONEntity {

  def geometry: Geometry

  def name: String

  def toJson: JsObject

  /**
    * Used to output the GeoJson format
    *
    * @return
    */
  def toPropertyMap: Map[String, AnyRef]

}
case class BrCountryEntity(geoID: String,
                           countryID: Int,
                           name: String,
                           geometry: Geometry
                       ) extends IBrGeoJSONEntity {
  override def toJson: JsObject =
    Json.obj("geoID" -> geoID, "countryID" -> countryID,  "name" -> name)

  override val level: TypeLevel = BrCountryLevel

  //create country level? Since we will have more than one?
  override val parentLevel: TypeLevel = BrCountryLevel

  override val key: Long = countryID.toLong

  //TODO country code
  override val parentKey: Long = countryID.toLong

  /**
    * Used to output the GeoJson format
    *
    * @return
    */
  override def toPropertyMap: Map[String, AnyRef] = Map[String, AnyRef](
    "geoID" -> geoID,
    "countryID" -> Int.box(countryID),
    "name" -> name)
}
case class BrStateEntity(geoID: String,
                         countryID: Int,
                         countryName: String,
                         stateID: Int,
                         name: String,
                         geometry: Geometry
                        ) extends IBrGeoJSONEntity {
  override def toJson: JsObject =
    Json.obj("geoID" -> geoID, "countryID" -> countryName, "stateID" -> stateID, "name" -> name)

  override val level: TypeLevel = BrStateLevel

  //create country level? Since we will have more than one?
  override val parentLevel: TypeLevel = BrCountryLevel

  override val key: Long = stateID.toLong

  //TODO country code
  override val parentKey: Long = "59470".toLong

  /**
    * Used to output the GeoJson format
    *
    * @return
    */
  override def toPropertyMap: Map[String, AnyRef] = Map[String, AnyRef](
    "geoID" -> geoID,
    "countryID" -> Int.box(59470),
    "countryName" -> countryName,
    "stateID" -> Int.box(stateID),
    "name" -> name)
}

case class BrCityEntity(geoID: String,
                        countryID: Int,
                        countryName: String,
                        stateID: Int,
                        stateName: String,
                        cityID: Int,
                        name: String,
                        geometry: Geometry
                       ) extends IBrGeoJSONEntity {

  override def toJson: JsObject =
    Json.obj("geoID" -> geoID, "stateID" -> stateID, "stateName" -> stateName, "cityID" -> cityID, "name" -> name)


  override val level: TypeLevel = BrCityLevel

  override val parentLevel: TypeLevel = BrStateLevel

  override val key: Long = cityID

  //BR_cities.json doesn't have stateID info, the cityID contains it in the first 2 digits
  override val parentKey: Long = stateID.toLong
 //TODO switch case for stateName ?

  override def toPropertyMap: Map[String, AnyRef] = Map[String, AnyRef](
    "geoID" -> geoID,
    "countryID" -> Int.box(countryID),
    "countryName" -> countryName,
    "stateID" -> Int.box(stateID),
    "stateName" -> stateName,
    "cityID" -> Int.box(cityID),
    "name" -> name)
}

object IBrGeoJSONEntity {

  def apply(map: Map[String, AnyRef], geometry: Geometry): IBrGeoJSONEntity = {
    map.get("cityID") match {
      case Some(obj) =>
        BrCityEntity(
          geoID = map.get("geoID").get.asInstanceOf[String],
          countryID = map.get("countryID").get.asInstanceOf[Int],
          countryName = map.get("countryName").get.asInstanceOf[String],
          stateID = map.get("stateID").get.asInstanceOf[Int],
          stateName = map.get("stateName").get.asInstanceOf[String],
          cityID = map.get("cityID").get.asInstanceOf[Int],
          name = map.get("name").get.asInstanceOf[String],
          geometry
        )
      case None => map.get("stateID") match {
        case Some(obj) =>
        BrStateEntity(
          geoID = map.get("geoID").get.asInstanceOf[String],
          countryID = map.get("countryID").get.asInstanceOf[Int],
          countryName = map.get("countryName").get.asInstanceOf[String],
          stateID = map.get("stateID").get.asInstanceOf[Int],
          name = map.get("name").get.asInstanceOf[String],
          geometry
        )
      case None =>
        BrCountryEntity(
          geoID = map.get("geoID").get.asInstanceOf[String],
          countryID = map.get("countryID").get.asInstanceOf[Int],
          name = map.get("name").get.asInstanceOf[String],
          geometry
        )
    }
  }}
}

